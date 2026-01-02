import sqlite3
import psycopg2
import psycopg2.extras
from dateutil import parser as date_parser
from datetime import datetime, timezone
import sys
import os
import time
import json
import getpass

# UI Bibliotheken
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich import box
import questionary

# ================= KONFIGURATION =================
DEFAULT_CONFIG = {
    "sqlite_path": "home-assistant_v2.db",
    "pg_host": "localhost",
    "pg_port": "5432",
    "pg_db": "homeassistant",
    "pg_user": "homeassistant",
    "pg_pass": "",
    "batch_size": 10000,
    "seq_buffer": 50000,
    "timescale_enabled": False
}

CONFIG_FILE = "config.json"
PROGRESS_FILE = "migration_progress.json"
ERROR_LOG_FILE = "migration_errors.log"

console = Console()

TABLES_CONF = [
    {"name": "event_data", "pk": "data_id"},
    {"name": "event_types", "pk": "event_type_id"},
    {"name": "events", "pk": "event_id", "time_col": "time_fired"},
    {"name": "recorder_runs", "pk": "run_id", "time_col": "start"},
    {"name": "state_attributes", "pk": "attributes_id"},
    {"name": "states_meta", "pk": "metadata_id"},
    {"name": "states", "pk": "state_id", "time_col": "last_updated", "drop_fk": ["states_old_state_id_fkey"]},
    {"name": "statistics_meta", "pk": "id"},
    {"name": "statistics_runs", "pk": "run_id", "time_col": "start"},
    {"name": "statistics", "pk": "id", "time_col": "start", "segment_by": "metadata_id"},
    {"name": "statistics_short_term", "pk": "id", "time_col": "start", "segment_by": "metadata_id"},
    {"name": "schema_changes", "pk": "change_id"},
]

DEPENDENCIES = {
    "events": ["event_data", "event_types"],
    "states": ["states_meta", "state_attributes"],
    "statistics": ["statistics_meta"],
    "statistics_short_term": ["statistics_meta"],
}

TABLE_MAP = {t["name"]: t for t in TABLES_CONF}
TIME_COL_KEYWORDS = ["time", "last_", "created", "start", "end", "changed"]

# ================= IO & UTILS =================

def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, 'r') as f: 
                data = json.load(f)
                return data if data else {}
        except: return {}
    return {}

def save_json(path, data):
    try:
        with open(path, 'w') as f: json.dump(data, f, indent=2)
    except Exception as e:
        console.print(f"[red]Fehler beim Speichern von {path}: {e}[/red]")

def save_progress(table_name, last_id):
    data = load_json(PROGRESS_FILE)
    data[table_name] = last_id
    save_json(PROGRESS_FILE, data)

def log_error_to_file(table_name, error_msg, sql_query, batch_sample):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] FEHLER in Tabelle '{table_name}'\n")
            f.write(f"MELDUNG: {error_msg}\n")
            f.write(f"SQL QUERY: {sql_query}\n")
            f.write(f"DATEN (Auszug):\n")
            for row in batch_sample[:3]:
                f.write(f"  {str(row)}\n")
            f.write("-" * 80 + "\n")
    except: pass

def format_bytes(size):
    power = 2**10
    n = 0
    power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"

# ================= DB MANAGERS =================

class PostgresManager:
    def __init__(self, config, fast_mode=True):
        self.cfg = config
        self.fast_mode = fast_mode
        self.conn = None

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                host=self.cfg["pg_host"], port=self.cfg["pg_port"], 
                database=self.cfg["pg_db"], user=self.cfg["pg_user"], password=self.cfg["pg_pass"]
            )
            with self.conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC';")
                if self.fast_mode:
                    cur.execute("SET session_replication_role = 'replica';")
            self.conn.commit()
            return self.conn
        except Exception as e:
            console.print(f"[bold red]FATAL: Postgres Verbindung fehlgeschlagen:[/bold red] {e}")
            sys.exit(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if self.fast_mode:
                    with self.conn.cursor() as cur:
                        cur.execute("SET session_replication_role = 'origin';")
                    self.conn.commit()
            except: pass
            self.conn.close()

def connect_sqlite_ro(path):
    if not os.path.exists(path):
        console.print(f"[bold red]FATAL: SQLite Datei fehlt:[/bold red] {path}")
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=60)
    except:
        conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    return conn

def get_max_id(cursor, table, pk_col):
    try:
        cursor.execute(f"SELECT MAX({pk_col}) FROM {table}")
        res = cursor.fetchone()[0]
        return res if res is not None else 0
    except: return 0

def update_sequence_if_needed(p_cur, table_name, pk_col, max_src_id, buffer_size):
    try:
        p_cur.execute(f"SELECT pg_get_serial_sequence('public.{table_name}', '{pk_col}')")
        res = p_cur.fetchone()
        if not res or not res[0]: return False
        
        seq_name = res[0]
        p_cur.execute(f"SELECT last_value FROM {seq_name}")
        curr_seq = p_cur.fetchone()[0]
        
        target = max_src_id + buffer_size
        if target > curr_seq:
            p_cur.execute(f"SELECT setval('{seq_name}', {target}, false)")
            return True, curr_seq, target
        return False, curr_seq, target
    except: return False, 0, 0

def get_blocking_constraints(p_cur, table_name):
    try:
        p_cur.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE schemaname = 'public' AND tablename = %s AND indexdef LIKE '%UNIQUE%'
        """, (table_name,))
        return [row[0] for row in p_cur.fetchall()]
    except: return []

# ================= CORE LOGIC =================

def process_value(val, target_type):
    """Konvertiert SQLite Werte basierend auf Postgres Ziel-Typ."""
    if val is None: return None
    tt = target_type.lower()

    if 'json' in tt: return val
    
    if 'boolean' in tt:
        if isinstance(val, int): return bool(val)
        if isinstance(val, str): return val.lower() in ('true', '1', 't', 'y', 'yes')
        return val
        
    if 'timestamp' in tt or 'date' in tt:
        if isinstance(val, (int, float)):
            try: return datetime.fromtimestamp(val, tz=timezone.utc)
            except: return None
        elif isinstance(val, str):
            try:
                dt = date_parser.parse(val)
                if dt.tzinfo is None: return dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except: return None
            
    if 'int' in tt or 'serial' in tt:
        if isinstance(val, float): return int(val)
        if isinstance(val, str):
            try: return int(float(val))
            except: return None
        return val
        
    if 'char' in tt or 'text' in tt:
        s_val = str(val)
        if '\0' in s_val: return s_val.replace('\0', '')
        return s_val
        
    return val

def migrate_single_table(s_conn, p_conn, tbl_conf, cfg, progress_map, rich_progress, task_id):
    table_name = tbl_conf['name']
    pk_col = tbl_conf.get('pk')
    
    # 1. Metadaten Quellen
    s_cur = s_conn.cursor()
    try:
        s_cur.execute(f"PRAGMA table_info({table_name})")
        s_cols = [r['name'] for r in s_cur.fetchall()]
    except: return {"status": False, "msg": "Source table missing"}
    finally: s_cur.close()

    with p_conn.cursor() as p_cur:
        p_cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (table_name,))
        col_types = {row[0]: row[1] for row in p_cur.fetchall()}

    common_cols = [c for c in s_cols if c in col_types]
    if not common_cols: return {"status": False, "msg": "No common columns"}

    # 2. Constraints Check (für User-Info)
    with p_conn.cursor() as check_cur:
        constraints = get_blocking_constraints(check_cur, table_name)
    if constraints:
        console.print(f"   [dim]ℹ Duplikat-Filter für {table_name}: {', '.join(constraints)}[/dim]")

    # 3. Query Build
    quoted_cols = [f'"{c}"' for c in common_cols]
    placeholders = ["%s"] * len(common_cols)
    conflict_sql = "ON CONFLICT DO NOTHING"
    insert_sql = f"INSERT INTO public.{table_name} ({', '.join(quoted_cols)}) VALUES ({', '.join(placeholders)}) {conflict_sql}"

    # 4. Status Check
    start_id = progress_map.get(table_name, 0)
    s_cur = s_conn.cursor()
    max_src_id = get_max_id(s_cur, table_name, pk_col) if pk_col else 0
    
    rich_progress.update(task_id, description=f"[cyan]Zähle {table_name}...", total=None)
    total_rows = 0
    
    if pk_col:
        # Resume Hinweis
        if start_id > 0 and start_id < max_src_id:
            console.print(f"   [yellow]⚠ Setze fort ab ID {start_id} (überspringe erledigte Datensätze)[/yellow]")
            s_cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {pk_col} > ?", (start_id,))
            total_rows = s_cur.fetchone()[0]
        else:
            s_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = s_cur.fetchone()[0]
    else:
        s_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = s_cur.fetchone()[0]

    if total_rows == 0:
        rich_progress.update(task_id, description=f"[green]{table_name} (Aktuell)", completed=100, total=100)
        s_cur.close()
        return {"status": True, "inserted": 0, "skipped": 0}

    rich_progress.update(task_id, description=f"[bold cyan]{table_name}", total=total_rows, completed=0)

    # 5. Batch Loop
    p_cur = p_conn.cursor()
    curr_off = start_id
    batch_size = cfg["batch_size"]
    stats_inserted = 0
    stats_skipped = 0
    
    try:
        while True:
            if pk_col:
                s_cur.execute(f"SELECT {', '.join(quoted_cols)} FROM {table_name} WHERE {pk_col} > ? AND {pk_col} <= ? ORDER BY {pk_col} ASC LIMIT ?", (curr_off, max_src_id, batch_size))
            else:
                s_cur.execute(f"SELECT {', '.join(quoted_cols)} FROM {table_name} LIMIT ? OFFSET ?", (batch_size, curr_off))

            rows = s_cur.fetchall()
            if not rows: break

            batch_data = []
            last_id = curr_off
            
            for row in rows:
                clean_row = []
                for c in common_cols:
                    val = row[c]
                    if val is not None:
                        val = process_value(val, col_types[c])
                    clean_row.append(val)
                batch_data.append(clean_row)
                if pk_col: last_id = row[pk_col]

            psycopg2.extras.execute_batch(p_cur, insert_sql, batch_data, page_size=batch_size)
            
            # Stats berechnen
            batch_inserted = p_cur.rowcount if p_cur.rowcount >= 0 else 0
            skipped_in_batch = len(batch_data) - batch_inserted
            stats_inserted += batch_inserted
            stats_skipped += skipped_in_batch
            
            rich_progress.update(task_id, description=f"[bold cyan]{table_name}[/] [dim](+{stats_inserted} | Skip: {stats_skipped})[/]")

            p_conn.commit()
            if pk_col: save_progress(table_name, last_id)
            
            rich_progress.update(task_id, advance=len(rows))
            curr_off = last_id
            if not pk_col: curr_off += len(rows)

        # Sequenz am Ende anpassen
        if pk_col:
            update_sequence_if_needed(p_cur, table_name, pk_col, max_src_id, cfg['seq_buffer'])
            p_conn.commit()

        return {"status": True, "inserted": stats_inserted, "skipped": stats_skipped}

    except Exception as e:
        p_conn.rollback()
        log_error_to_file(table_name, str(e), insert_sql, batch_data)
        return {"status": False, "msg": str(e)}
    finally:
        p_cur.close(); s_cur.close()

# ================= MENUS & ACTIONS =================

def action_create_config():
    console.print(Panel("[bold]Konfiguration erstellen[/bold]", box=box.ROUNDED))
    current = load_json(CONFIG_FILE) or DEFAULT_CONFIG.copy()

    config = {}
    config['sqlite_path'] = questionary.path("Pfad zur SQLite Datei:", default=current.get('sqlite_path')).ask()
    config['pg_host'] = questionary.text("Postgres Host:", default=current.get('pg_host')).ask()
    config['pg_port'] = questionary.text("Postgres Port:", default=current.get('pg_port')).ask()
    config['pg_db'] = questionary.text("DB Name:", default=current.get('pg_db')).ask()
    config['pg_user'] = questionary.text("User:", default=current.get('pg_user')).ask()
    config['pg_pass'] = questionary.password("Passwort:", default=current.get('pg_pass')).ask()
    config['batch_size'] = int(questionary.text("Batch Size:", default=str(current.get('batch_size'))).ask())
    config['seq_buffer'] = int(questionary.text("Sequenz Puffer:", default=str(current.get('seq_buffer'))).ask())
    config['timescale_enabled'] = questionary.confirm("TimescaleDB nutzen?", default=current.get('timescale_enabled')).ask()

    save_json(CONFIG_FILE, config)
    console.print(f"[green]Konfiguration gespeichert in {CONFIG_FILE}[/green]")
    questionary.press_any_key_to_continue().ask()

def action_reset_progress():
    console.print(Panel("[bold]Fortschritt zurücksetzen[/bold]", box=box.ROUNDED))
    data = load_json(PROGRESS_FILE)
    
    if not data:
        console.print("[yellow]Keine Fortschrittsdaten gefunden.[/yellow]")
        questionary.press_any_key_to_continue().ask()
        return

    choice = questionary.select(
        "Was möchten Sie zurücksetzen?",
        choices=[
            "Alles zurücksetzen (Datei löschen)",
            "Einzelne Tabellen auswählen",
            "Abbrechen"
        ]
    ).ask()

    if choice == "Abbrechen": return

    if choice.startswith("Alles"):
        try:
            os.remove(PROGRESS_FILE)
            console.print("[green]Gesamter Fortschritt gelöscht. Migration startet bei 0.[/green]")
        except Exception as e:
            console.print(f"[red]Fehler: {e}[/red]")
            
    elif choice.startswith("Einzelne"):
        choices = [questionary.Choice(k) for k in data.keys()]
        selected = questionary.checkbox("Welche Tabellen zurücksetzen?", choices=choices).ask()
        
        if selected:
            for t in selected:
                if t in data: del data[t]
            save_json(PROGRESS_FILE, data)
            console.print(f"[green]Fortschritt für {len(selected)} Tabellen zurückgesetzt.[/green]")
    
    questionary.press_any_key_to_continue().ask()

def action_sequence_reset(config):
    console.print(Panel("[bold]Pre-Flight Check: Sequenzen[/bold]", box=box.ROUNDED))
    s_conn = connect_sqlite_ro(config["sqlite_path"])
    if not s_conn: return

    with console.status("[bold green]Verbinde mit Postgres...") as status:
        with PostgresManager(config, fast_mode=False) as p_conn:
            p_cur = p_conn.cursor()
            table = Table(box=box.SIMPLE)
            table.add_column("Tabelle", style="cyan")
            table.add_column("Status", style="magenta")
            
            for tbl in TABLES_CONF:
                name = tbl['name']
                pk = tbl.get('pk')
                if not pk: continue
                
                max_sqlite = get_max_id(s_conn.cursor(), name, pk)
                updated, curr, target = update_sequence_if_needed(p_cur, name, pk, max_sqlite, config['seq_buffer'])
                
                if updated:
                    table.add_row(name, f"UPDATE: {curr} -> {target}")
                    p_conn.commit()
                else:
                    table.add_row(name, f"OK ({curr} > {target})")
            p_cur.close()
    
    console.print(table)
    s_conn.close()
    questionary.press_any_key_to_continue().ask()

def action_convert_schema(config):
    console.print(Panel("[bold]TimescaleDB Konvertierung[/bold]", box=box.ROUNDED))
    
    candidates = [t for t in TABLES_CONF if "time_col" in t]
    choices = [questionary.Choice(t['name'], value=t) for t in candidates]
    targets = questionary.checkbox("Wähle Tabellen:", choices=choices).ask()

    if not targets: return

    if any(t['name'] == 'states' for t in targets):
        console.print("[bold yellow]WARNUNG:[/bold yellow] Tabelle 'states' gewählt. Foreign Keys werden gelöscht.")
        if not questionary.confirm("Fortfahren?").ask(): return

    with PostgresManager(config, fast_mode=False) as p_conn:
        c = p_conn.cursor()
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), transient=True) as progress:
            task = progress.add_task("Verarbeite...", total=len(targets))
            for tbl in targets:
                name = tbl['name']
                progress.update(task, description=f"Konvertiere {name}...")
                try:
                    if "drop_fk" in tbl:
                        for fk in tbl["drop_fk"]:
                            try: c.execute(f"ALTER TABLE {name} DROP CONSTRAINT IF EXISTS {fk};")
                            except: pass
                    try: c.execute(f"ALTER TABLE {name} DROP CONSTRAINT {name}_pkey CASCADE;")
                    except: pass
                    try: c.execute(f"ALTER TABLE {name} ADD PRIMARY KEY ({tbl['pk']}, {tbl['time_col']});")
                    except: pass
                    
                    c.execute(f"SELECT create_hypertable('{name}', '{tbl['time_col']}', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, migrate_data => TRUE);")
                    
                    if "segment_by" in tbl:
                        c.execute(f"ALTER TABLE {name} SET (timescaledb.compress, timescaledb.compress_segmentby = '{tbl['segment_by']}');")
                    else:
                        c.execute(f"ALTER TABLE {name} SET (timescaledb.compress);")
                    c.execute(f"SELECT add_compression_policy('{name}', INTERVAL '14 days');")
                    p_conn.commit()
                    console.print(f"[green]✓ {name} konvertiert[/green]")
                except Exception as e:
                    p_conn.rollback(); console.print(f"[red]✗ {name}: {e}[/red]")
                progress.advance(task)
    questionary.press_any_key_to_continue().ask()

def run_migration_plan(config, plan):
    s_conn = connect_sqlite_ro(config["sqlite_path"])
    if not s_conn: return

    report_data = []
    with PostgresManager(config, fast_mode=True) as p_conn:
        progress_data = load_json(PROGRESS_FILE)
        rich_progress = Progress(
            SpinnerColumn(), TextColumn("[bold blue]{task.description}"), BarColumn(),
            TaskProgressColumn(), TextColumn("•"), TimeRemainingColumn(), TextColumn("• {task.completed}/{task.total}")
        )
        with rich_progress:
            for t_name in plan:
                task_id = rich_progress.add_task(f"Warte auf {t_name}...", start=False)
                rich_progress.start_task(task_id)
                res = migrate_single_table(s_conn, p_conn, TABLE_MAP[t_name], config, progress_data, rich_progress, task_id)
                
                if res["status"]:
                    report_data.append({"name": t_name, "status": "OK", "ins": res["inserted"], "skip": res["skipped"]})
                else:
                    report_data.append({"name": t_name, "status": "FAIL", "msg": res["msg"]})
                    console.print(f"[bold red]Abbruch bei {t_name}: {res['msg']}[/bold red]")
                    break
    s_conn.close()
    
    console.print("")
    rpt = Table(title="Migration Report", box=box.SIMPLE)
    rpt.add_column("Tabelle", style="cyan"); rpt.add_column("Status", style="bold")
    rpt.add_column("Neu", style="green", justify="right"); rpt.add_column("Skip", style="yellow", justify="right")
    
    for i in report_data:
        if i["status"] == "OK": rpt.add_row(i["name"], "[green]OK[/]", str(i["ins"]), str(i["skip"]))
        else: rpt.add_row(i["name"], "[red]FAIL[/]", "-", "-")
    console.print(rpt)

    if any("statistics" in t for t in plan) and config["timescale_enabled"]:
        if questionary.confirm("TimescaleDB Optimierung jetzt starten?").ask(): action_convert_schema(config)
    else: questionary.press_any_key_to_continue().ask()

def action_migration_menu(config):
    choice = questionary.select("Migrationstyp wählen:", choices=["Alle Tabellen migrieren", "Selektive Auswahl", "Zurück"]).ask()
    if choice == "Zurück": return
    
    target_plan = []
    if choice.startswith("Alle"): target_plan = [t['name'] for t in TABLES_CONF]
    else:
        choices = [questionary.Choice(t['name']) for t in TABLES_CONF]
        target_plan = questionary.checkbox("Wähle Tabellen:", choices=choices).ask()
        if not target_plan: return
        
        selected_set = set(target_plan); missing = set()
        for t in target_plan:
            if t in DEPENDENCIES:
                for p in DEPENDENCIES[t]:
                    if p not in selected_set: missing.add(p)
        if missing:
            console.print(f"[yellow]Warnung: Es fehlen Eltern-Tabellen:[/yellow] {', '.join(missing)}")
            opt = questionary.select("Wie verfahren?", choices=["Fehlende hinzufügen", "Ignorieren", "Abbruch"]).ask()
            if opt == "Abbruch": return
            if opt.startswith("Fehlende"):
                 combined = selected_set | missing
                 target_plan = [t['name'] for t in TABLES_CONF if t['name'] in combined]

    run_migration_plan(config, target_plan)

def action_show_stats(config):
    console.print(Panel("[bold]Datenbank Vergleich[/bold]", box=box.ROUNDED))
    s_conn = connect_sqlite_ro(config["sqlite_path"])
    if not s_conn: return

    with console.status("Lade Statistiken..."):
        with PostgresManager(config, fast_mode=False) as p_conn:
            p_cur = p_conn.cursor(); s_cur = s_conn.cursor()
            try:
                s_size = os.path.getsize(config["sqlite_path"])
                p_cur.execute("SELECT pg_database_size(current_database());"); p_size = p_cur.fetchone()[0]
            except: s_size=0; p_size=0

            table = Table(title=f"Größe: SQLite {format_bytes(s_size)} vs Postgres {format_bytes(p_size)}")
            table.add_column("Tabelle", style="cyan"); table.add_column("Zeilen (SQLite)", justify="right")
            table.add_column("Zeilen (PG)", justify="right"); table.add_column("Timescale?", justify="center")

            for tbl in TABLES_CONF:
                name = tbl['name']
                try: s_cur.execute(f"SELECT COUNT(*) FROM {name}"); s_c = f"{s_cur.fetchone()[0]:,}"
                except: s_c = "n/a"
                try:
                    p_cur.execute(f"SELECT COUNT(*) FROM {name}"); p_c = f"{p_cur.fetchone()[0]:,}"
                    p_cur.execute("SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name=%s", (name,))
                    ts = "✅" if p_cur.fetchone() else "-"
                except: p_c = "err"; ts = "?"
                table.add_row(name, s_c, p_c, ts)
    console.print(table); s_conn.close()
    questionary.press_any_key_to_continue().ask()

def action_maintenance(config):
    choice = questionary.select("Wartungsoption wählen:", 
        choices=["Sequenzen Reset (Pre-Flight)", "DB Status Check", "Postgres VACUUM", "Fortschritt zurücksetzen", "Zurück"]).ask()
    
    if choice.startswith("Sequenzen"): action_sequence_reset(config)
    elif choice.startswith("DB Status"): action_show_stats(config)
    elif choice.startswith("Fortschritt"): action_reset_progress()
    elif choice.startswith("Postgres VACUUM"):
        if questionary.confirm("Starten?").ask():
            try:
                conn = psycopg2.connect(host=config["pg_host"], port=config["pg_port"], database=config["pg_db"], user=config["pg_user"], password=config["pg_pass"])
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                with console.status("Führe VACUUM ANALYZE aus..."): cur.execute("VACUUM ANALYZE;")
                console.print("[green]Fertig.[/green]"); conn.close()
            except Exception as e: console.print(f"[red]Fehler: {e}[/red]")
            questionary.press_any_key_to_continue().ask()

# ================= MAIN =================

def main():
    console.clear()
    console.print(Panel.fit("[bold blue]Home Assistant Migration Tool[/bold blue]\n[dim]Next-Gen Edition (v10)[/dim]", box=box.DOUBLE))

    if not os.path.exists(CONFIG_FILE):
        if questionary.confirm("Keine Konfiguration gefunden. Jetzt erstellen?").ask(): action_create_config()
        else: return

    try:
        with open(CONFIG_FILE) as f: config = json.load(f)
    except:
        console.print("[red]Konfiguration defekt.[/red]"); return

    while True:
        console.print("")
        choice = questionary.select("Hauptmenü", choices=["Migration starten", "Wartung & Diagnose", "TimescaleDB Optimierung", "Konfiguration bearbeiten", "Beenden"]).ask()
        if choice == "Beenden": sys.exit(0)
        elif choice == "Konfiguration bearbeiten": action_create_config()
        elif choice == "Wartung & Diagnose": action_maintenance(config)
        elif choice == "TimescaleDB Optimierung": action_convert_schema(config)
        elif choice == "Migration starten": action_migration_menu(config)

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: console.print("\n[yellow]Beendet durch Benutzer.[/yellow]")