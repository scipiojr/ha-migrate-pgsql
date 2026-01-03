import sqlite3
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
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
SKIP_LOG_FILE = "migration_skipped.log"

console = Console()

# --- UPDATE v33: Nutzung der _ts Spalten (Timestamp) statt der alten String-Spalten ---
TABLES_CONF = [
    {"name": "event_data", "pk": "data_id"},
    {"name": "event_types", "pk": "event_type_id"},
    {"name": "events", "pk": "event_id", "time_col": "time_fired_ts"}, 
    {"name": "recorder_runs", "pk": "run_id", "time_col": "start_ts"},
    {"name": "state_attributes", "pk": "attributes_id"},
    {"name": "states_meta", "pk": "metadata_id"},
    {"name": "states", "pk": "state_id", "time_col": "last_updated_ts", "drop_fk": ["states_old_state_id_fkey"]},
    {"name": "statistics_meta", "pk": "id"},
    {"name": "statistics_runs", "pk": "run_id", "time_col": "start_ts"},
    {"name": "statistics", "pk": "id", "time_col": "start_ts", "segment_by": "metadata_id"},
    {"name": "statistics_short_term", "pk": "id", "time_col": "start_ts", "segment_by": "metadata_id"},
    {"name": "schema_changes", "pk": "change_id"},
]

DEPENDENCIES = {
    "events": ["event_data", "event_types"],
    "states": ["states_meta", "state_attributes"],
    "statistics": ["statistics_meta"],
    "statistics_short_term": ["statistics_meta"],
}

TABLE_MAP = {t["name"]: t for t in TABLES_CONF}

# ================= IO & UTILS =================

def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                return data if data else {}
        except:
            return {}
    return {}

def save_json(path, data):
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        console.print(f"[red]Fehler beim Speichern von {path}: {e}[/red]")

def save_progress(table_name, last_id):
    data = load_json(PROGRESS_FILE)
    data[table_name] = last_id
    save_json(PROGRESS_FILE, data)

def log_error_to_file(table_name, error_msg, sql_query, batch_sample):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8", errors='replace') as f:
            f.write(f"[{timestamp}] FEHLER in Tabelle '{table_name}'\n")
            f.write(f"MELDUNG: {error_msg}\n")
            f.write(f"SQL QUERY: {sql_query[:500] if sql_query else 'N/A'}...\n")
            f.write("-" * 80 + "\n")
    except:
        pass

def condense_ids_to_ranges(id_list):
    if not id_list:
        return []
    try:
        sorted_ids = sorted([int(x) for x in id_list])
    except ValueError:
        return [str(x) for x in id_list]
    
    ranges = []
    if not sorted_ids:
        return ranges
    
    range_start = sorted_ids[0]
    prev_id = sorted_ids[0]
    
    for curr_id in sorted_ids[1:]:
        if curr_id == prev_id + 1:
            prev_id = curr_id
        else:
            if range_start == prev_id:
                ranges.append(str(range_start))
            else:
                ranges.append(f"{range_start}-{prev_id}")
            range_start = curr_id
            prev_id = curr_id
            
    if range_start == prev_id:
        ranges.append(str(range_start))
    else:
        ranges.append(f"{range_start}-{prev_id}")
        
    return ranges

def log_skips_to_file(table_name, skipped_ids, reason="Duplicate Key"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        ranges = condense_ids_to_ranges(skipped_ids)
        range_str = ", ".join(ranges)
        if len(range_str) > 2000:
            range_str = range_str[:2000] + " ... (gekürzt)"
        with open(SKIP_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {table_name}: Übersprungen {len(skipped_ids)} Zeilen.\n")
            f.write(f"  Grund: {reason}\n")
            f.write(f"  IDs: {range_str}\n")
            f.write("-" * 40 + "\n")
    except Exception as e:
        console.print(f"[red]Fehler beim Schreiben des Skip-Logs: {e}[/red]")

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
            except:
                pass
            self.conn.close()

def connect_sqlite_ro(path):
    if not os.path.exists(path):
        console.print(f"[bold red]FATAL: SQLite Datei fehlt:[/bold red] {path}")
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=300)
    except:
        conn = sqlite3.connect(path, timeout=300)
    conn.row_factory = sqlite3.Row
    return conn

def connect_sqlite_rw(path):
    if not os.path.exists(path):
        console.print(f"[bold red]FATAL: SQLite Datei fehlt:[/bold red] {path}")
        return None
    try:
        conn = sqlite3.connect(path, timeout=600)
        return conn
    except Exception as e:
        console.print(f"[red]Fehler beim Öffnen von SQLite (RW): {e}[/red]")
        return None

def get_max_id(cursor, table, pk_col):
    try:
        cursor.execute(f"SELECT MAX({pk_col}) FROM {table}")
        res = cursor.fetchone()[0]
        return res if res is not None else 0
    except:
        return 0

def update_sequence_if_needed(p_cur, table_name, pk_col, max_src_id, buffer_size):
    try:
        p_cur.execute(f"SELECT pg_get_serial_sequence('public.{table_name}', '{pk_col}')")
        res = p_cur.fetchone()
        if not res or not res[0]:
            return False, 0, 0
        
        seq_name = res[0]
        p_cur.execute(f"SELECT last_value FROM {seq_name}")
        curr_seq = p_cur.fetchone()[0]
        
        target = max_src_id + buffer_size
        if target > curr_seq:
            p_cur.execute(f"SELECT setval('{seq_name}', {target}, false)")
            return True, curr_seq, target
        return False, curr_seq, target
    except:
        return False, 0, 0

def get_blocking_constraints(p_cur, table_name):
    try:
        p_cur.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE schemaname = 'public' AND tablename = %s AND indexdef LIKE '%UNIQUE%'
        """, (table_name,))
        return [row[0] for row in p_cur.fetchall()]
    except:
        return []

def get_extended_db_stats(p_cur, table_name, pk_col, s_max):
    stats = {"resume": 0, "barrier": None, "head": 0, "seq": 0}
    if not pk_col:
        return stats
    
    try:
        if s_max > 0:
            p_cur.execute(f"SELECT MAX({pk_col}) FROM public.{table_name} WHERE {pk_col} < %s", (s_max,))
            res = p_cur.fetchone()[0]
            stats["resume"] = res if res else 0
        
        if s_max > 0:
            p_cur.execute(f"SELECT MIN({pk_col}) FROM public.{table_name} WHERE {pk_col} > %s", (s_max,))
            res = p_cur.fetchone()[0]
            stats["barrier"] = res
            
        p_cur.execute(f"SELECT MAX({pk_col}) FROM public.{table_name}")
        res = p_cur.fetchone()[0]
        stats["head"] = res if res else 0
        
        p_cur.execute(f"SELECT pg_get_serial_sequence('public.{table_name}', '{pk_col}')")
        seq_res = p_cur.fetchone()
        if seq_res and seq_res[0]:
            p_cur.execute(f"SELECT last_value FROM {seq_res[0]}")
            stats["seq"] = p_cur.fetchone()[0]
    except:
        pass
        
    return stats

# ================= CORE LOGIC: CONVERTERS =================

def _conv_noop(val):
    return val

def _conv_bool(val):
    if val is None: return None
    if isinstance(val, int): return bool(val)
    if isinstance(val, str): return val.lower() in ('true', '1', 't', 'y', 'yes')
    return val

def _conv_timestamp(val):
    if val is None: return None
    if isinstance(val, (int, float)):
        try: return datetime.fromtimestamp(val, tz=timezone.utc)
        except: return None
    elif isinstance(val, str):
        try:
            dt = date_parser.parse(val)
            if dt.tzinfo is None: return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except: return None
    return None

def _conv_int(val):
    if val is None: return None
    if isinstance(val, float): return int(val)
    if isinstance(val, str):
        try: return int(float(val))
        except: return None
    return val

def _conv_str_clean(val):
    if val is None: return None
    s_val = str(val)
    if '\0' in s_val: return s_val.replace('\0', '')
    return s_val

def get_converter_for_type(target_type):
    tt = target_type.lower()
    if 'boolean' in tt: return _conv_bool
    if 'timestamp' in tt or 'date' in tt: return _conv_timestamp
    if 'int' in tt or 'serial' in tt: return _conv_int
    if 'char' in tt or 'text' in tt: return _conv_str_clean
    return _conv_noop

def analyze_skips(p_cur, table_name, pk_col, batch_data, common_cols):
    if not pk_col:
        return []
    try:
        try:
            pk_idx = common_cols.index(pk_col)
        except ValueError:
            return []
        ids_in_batch = [row[pk_idx] for row in batch_data if row[pk_idx] is not None]
        if not ids_in_batch:
            return []
        query = f"SELECT {pk_col} FROM public.{table_name} WHERE {pk_col} = ANY(%s)"
        p_cur.execute(query, (ids_in_batch,))
        return [r[0] for r in p_cur.fetchall()]
    except Exception as e:
        console.print(f"[red]Fehler bei Skip-Analyse: {e}[/red]")
        return []

def run_preflight_analysis(s_conn, p_conn, plan):
    console.print(Panel("[bold]Pre-Flight: Gap-Analyse & Synchronisation[/bold]", box=box.ROUNDED))
    table = Table(box=box.SIMPLE)
    table.add_column("Tabelle", style="cyan")
    table.add_column("SQ Max", justify="right", header_style="bold green")
    table.add_column("PG Resume", justify="right", style="dim")
    table.add_column("PG Barrier", justify="right", style="bold red")
    table.add_column("PG Head", justify="right", style="dim")
    table.add_column("Seq", justify="right")
    table.add_column("Start-Aktion", style="bold")

    progress_data = load_json(PROGRESS_FILE)
    s_cur = s_conn.cursor()
    p_cur = p_conn.cursor()

    with console.status("Analysiere Tabellenstatus..."):
        for t_name in plan:
            tbl_conf = TABLE_MAP[t_name]
            pk_col = tbl_conf.get('pk')
            
            s_max = 0
            stats = {"resume": 0, "barrier": None, "head": 0, "seq": 0}
            action = "Full Scan"
            
            if pk_col:
                try:
                    s_max = get_max_id(s_cur, t_name, pk_col)
                except:
                    s_max = 0
                
                stats = get_extended_db_stats(p_cur, t_name, pk_col, s_max)
                
                file_start = progress_data.get(t_name, 0)
                start_point = max(file_start, stats["resume"])
                
                if start_point >= s_max and s_max > 0:
                    action = "[green]✅ Fertig[/green]"
                elif start_point > 0:
                    action = f"[yellow]⏩ {start_point:,}[/yellow]"
                else:
                    action = "[blue]▶️ 0[/blue]"
                
                barrier_str = f"{stats['barrier']:,}" if stats['barrier'] else "-"
                
                table.add_row(
                    t_name, 
                    f"{s_max:,}", 
                    f"{stats['resume']:,}", 
                    barrier_str,
                    f"{stats['head']:,}",
                    f"{stats['seq']:,}",
                    action
                )
            else:
                table.add_row(t_name, "-", "-", "-", "-", "-", "Full Scan (No PK)")

    p_cur.close()
    console.print(table)
    console.print("")

def migrate_single_table(s_conn, p_conn, tbl_conf, cfg, progress_map, rich_progress, task_id):
    table_name = tbl_conf['name']
    pk_col = tbl_conf.get('pk')
    
    batch_data = []
    insert_sql = None
    
    s_cur = s_conn.cursor()
    try:
        s_cur.execute(f"PRAGMA table_info({table_name})")
        s_cols = [r['name'] for r in s_cur.fetchall()]
    except:
        return {"status": False, "msg": "Source table missing"}
    finally:
        s_cur.close()

    with p_conn.cursor() as p_cur:
        p_cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (table_name,))
        col_types = {row[0]: row[1] for row in p_cur.fetchall()}

    common_cols = [c for c in s_cols if c in col_types]
    if not common_cols:
        return {"status": False, "msg": "No common columns"}

    converters = [get_converter_for_type(col_types[c]) for c in common_cols]

    with p_conn.cursor() as check_cur:
        constraints = get_blocking_constraints(check_cur, table_name)
    if constraints:
        console.print(f"   [dim]ℹ Duplikat-Filter für {table_name}: {', '.join(constraints)}[/dim]")

    quoted_cols = [f'"{c}"' for c in common_cols]
    placeholders = ["%s"] * len(common_cols)
    conflict_sql = "ON CONFLICT DO NOTHING"
    insert_sql = f"INSERT INTO public.{table_name} ({', '.join(quoted_cols)}) VALUES %s {conflict_sql}"
    
    if pk_col:
        insert_sql += f" RETURNING {pk_col}"

    saved_start_id = progress_map.get(table_name, 0)
    s_cur = s_conn.cursor()
    
    rich_progress.update(task_id, description=f"[cyan]Analysiere {table_name}...", total=None)
    
    total_rows_abs = 0; total_rows_todo = 0; max_src_id = 0; final_start_id = saved_start_id
    p_cur = p_conn.cursor()

    try:
        s_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows_abs = s_cur.fetchone()[0]
        if pk_col:
            max_src_id = get_max_id(s_cur, table_name, pk_col)
            if final_start_id == 0 and max_src_id > 0:
                stats = get_extended_db_stats(p_cur, table_name, pk_col, max_src_id)
                if stats["resume"] > 0:
                    final_start_id = stats["resume"]

            if final_start_id > 0 and final_start_id < max_src_id:
                s_cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {pk_col} > ?", (final_start_id,))
                total_rows_todo = s_cur.fetchone()[0]
            elif final_start_id >= max_src_id and max_src_id > 0:
                total_rows_todo = 0
            else:
                total_rows_todo = total_rows_abs
        else:
            total_rows_todo = total_rows_abs
    except:
        pass

    already_done_count = total_rows_abs - total_rows_todo

    if total_rows_todo == 0:
        rich_progress.update(task_id, description=f"[green]{table_name} (Komplett)", completed=100, total=100)
        s_cur.close()
        p_cur.close()
        return {"status": True, "total": total_rows_abs, "inserted": 0, "skipped": 0, "previously_done": already_done_count}

    rich_progress.update(task_id, description=f"[bold cyan]{table_name}", total=total_rows_todo, completed=0)

    curr_off = final_start_id
    batch_size = cfg["batch_size"]
    stats_inserted = 0
    stats_skipped = 0
    
    pk_idx = -1
    if pk_col:
        try: pk_idx = common_cols.index(pk_col)
        except: pk_idx = -1

    try:
        while True:
            if pk_col:
                s_cur.execute(f"SELECT {', '.join(quoted_cols)} FROM {table_name} WHERE {pk_col} > ? AND {pk_col} <= ? ORDER BY {pk_col} ASC LIMIT ?", (curr_off, max_src_id, batch_size))
            else:
                s_cur.execute(f"SELECT {', '.join(quoted_cols)} FROM {table_name} LIMIT ? OFFSET ?", (batch_size, curr_off))

            rows = s_cur.fetchall()
            if not rows:
                break

            batch_data = []
            batch_ids = []
            last_id = curr_off
            
            for row in rows:
                clean_row = []
                for i, val in enumerate(row):
                    if val is not None:
                        val = converters[i](val)
                    clean_row.append(val)
                batch_data.append(clean_row)
                
                if pk_col:
                    last_id = row[pk_col]
                    if pk_idx >= 0:
                        batch_ids.append(clean_row[pk_idx])

            if pk_col:
                try:
                    returned_rows = execute_values(p_cur, insert_sql, batch_data, page_size=batch_size, fetch=True)
                    inserted_ids_set = set(r[0] for r in returned_rows)
                    batch_inserted = len(inserted_ids_set)
                except:
                    insert_sql_fallback = insert_sql.replace(f" RETURNING {pk_col}", "")
                    execute_values(p_cur, insert_sql_fallback, batch_data, page_size=batch_size)
                    batch_inserted = p_cur.rowcount
                    inserted_ids_set = set()

                if batch_inserted == 0 and p_cur.rowcount > 0:
                    batch_inserted = p_cur.rowcount
                
                skipped_in_batch = len(batch_data) - batch_inserted
                
                if skipped_in_batch > 0 and inserted_ids_set:
                    all_ids_set = set(batch_ids)
                    skipped_ids = list(all_ids_set - inserted_ids_set)
                    if skipped_ids:
                        log_skips_to_file(table_name, skipped_ids, reason="Konflikt: ID existiert bereits")
            else:
                execute_values(p_cur, insert_sql, batch_data, page_size=batch_size)
                batch_inserted = p_cur.rowcount
                if batch_inserted < 0: batch_inserted = 0
                skipped_in_batch = len(batch_data) - batch_inserted

            stats_inserted += batch_inserted
            stats_skipped += skipped_in_batch
            
            rich_progress.update(task_id, description=f"[bold cyan]{table_name}[/] [dim](+{stats_inserted} | Skip: {stats_skipped})[/]")

            p_conn.commit()
            if pk_col:
                save_progress(table_name, last_id)
            
            rich_progress.update(task_id, advance=len(rows))
            curr_off = last_id
            if not pk_col:
                curr_off += len(rows)

        if pk_col:
            update_sequence_if_needed(p_cur, table_name, pk_col, max_src_id, cfg['seq_buffer'])
            p_conn.commit()

        return {"status": True, "total": total_rows_abs, "inserted": stats_inserted, "skipped": stats_skipped, "previously_done": already_done_count}

    except Exception as e:
        p_conn.rollback()
        log_error_to_file(table_name, str(e), insert_sql, batch_data)
        return {"status": False, "msg": str(e)}
    finally:
        p_cur.close()
        s_cur.close()

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
    choice = questionary.select("Was möchten Sie zurücksetzen?", choices=["Alles zurücksetzen", "Einzelne Tabellen", "Abbrechen"]).ask()
    if choice == "Abbrechen":
        return
    if choice.startswith("Alles"):
        try:
            os.remove(PROGRESS_FILE)
            console.print("[green]Alles zurückgesetzt.[/green]")
        except Exception as e:
            console.print(f"[red]Fehler: {e}[/red]")
    elif choice.startswith("Einzelne"):
        selected = questionary.checkbox("Welche Tabellen?", choices=[questionary.Choice(k) for k in data.keys()]).ask()
        if selected:
            for t in selected: 
                if t in data:
                    del data[t]
            save_json(PROGRESS_FILE, data)
            console.print(f"[green]{len(selected)} Tabellen zurückgesetzt.[/green]")
    questionary.press_any_key_to_continue().ask()

def action_sequence_reset(config):
    console.print(Panel("[bold]Pre-Flight Check: Sequenzen[/bold]", box=box.ROUNDED))
    s_conn = connect_sqlite_ro(config["sqlite_path"])
    if not s_conn:
        return
    with console.status("[bold green]Verbinde mit Postgres...") as status:
        with PostgresManager(config, fast_mode=False) as p_conn:
            p_cur = p_conn.cursor()
            table = Table(box=box.SIMPLE)
            table.add_column("Tabelle", style="cyan")
            table.add_column("Status", style="magenta")
            for tbl in TABLES_CONF:
                name = tbl['name']
                pk = tbl.get('pk')
                if not pk:
                    continue
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
    choices = [questionary.Choice(t['name'], value=t) for t in TABLES_CONF if "time_col" in t]
    targets = questionary.checkbox("Wähle Tabellen:", choices=choices).ask()
    if not targets:
        return
    if any(t['name'] == 'states' for t in targets):
        console.print("[bold yellow]WARNUNG: 'states' gewählt. Foreign Keys werden gelöscht.[/bold yellow]")
        if not questionary.confirm("Fortfahren?").ask():
            return
    with PostgresManager(config, fast_mode=False) as p_conn:
        c = p_conn.cursor()
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), transient=True) as progress:
            task = progress.add_task("Verarbeite...", total=len(targets))
            for tbl in targets:
                name = tbl['name']
                progress.update(task, description=f"Konvertiere {name}...")
                
                # --- AUTO-CLEANUP v32/33 ---
                # Wir checken jetzt die _ts Spalte auf NULL
                time_col = tbl['time_col']
                try:
                    c.execute(f"SELECT COUNT(*) FROM {name} WHERE {time_col} IS NULL;")
                    null_count = c.fetchone()[0]
                    if null_count > 0:
                        progress.update(task, description=f"Lösche {null_count} defekte Zeilen (NULL {time_col})...")
                        c.execute(f"DELETE FROM {name} WHERE {time_col} IS NULL;")
                        p_conn.commit()
                except Exception as e:
                    console.print(f"[yellow]Warnung beim Bereinigen von {name}: {e}[/yellow]")
                
                try:
                    if "drop_fk" in tbl:
                        for fk in tbl["drop_fk"]:
                            try:
                                c.execute(f"SAVEPOINT drop_fk_{fk}")
                                c.execute(f"ALTER TABLE {name} DROP CONSTRAINT IF EXISTS {fk};")
                                c.execute(f"RELEASE SAVEPOINT drop_fk_{fk}")
                            except:
                                c.execute(f"ROLLBACK TO SAVEPOINT drop_fk_{fk}")

                    try:
                        c.execute(f"SAVEPOINT drop_pk_{name}")
                        c.execute(f"ALTER TABLE {name} DROP CONSTRAINT {name}_pkey CASCADE;")
                        c.execute(f"RELEASE SAVEPOINT drop_pk_{name}")
                    except:
                        c.execute(f"ROLLBACK TO SAVEPOINT drop_pk_{name}")

                    c.execute(f"ALTER TABLE {name} ADD PRIMARY KEY ({tbl['pk']}, {tbl['time_col']});")
                    
                    c.execute(f"SELECT create_hypertable('{name}', '{tbl['time_col']}', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, migrate_data => TRUE);")
                    
                    if "segment_by" in tbl:
                        c.execute(f"ALTER TABLE {name} SET (timescaledb.compress, timescaledb.compress_segmentby = '{tbl['segment_by']}');")
                    else:
                        c.execute(f"ALTER TABLE {name} SET (timescaledb.compress);")
                    
                    c.execute(f"SELECT add_compression_policy('{name}', INTERVAL '14 days');")
                    
                    p_conn.commit()
                    console.print(f"[green]✓ {name} konvertiert[/green]")
                except Exception as e:
                    p_conn.rollback()
                    console.print(f"[red]✗ {name}: {e}[/red]")
                progress.advance(task)
    questionary.press_any_key_to_continue().ask()

def run_migration_plan(config, plan):
    s_conn = connect_sqlite_ro(config["sqlite_path"])
    if not s_conn:
        return
    with PostgresManager(config, fast_mode=False) as p_conn:
        run_preflight_analysis(s_conn, p_conn, plan)
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
                    res["name"] = t_name
                    report_data.append(res)
                else:
                    report_data.append({"name": t_name, "status": False, "msg": res["msg"]})
                    console.print(f"[bold red]Abbruch bei {t_name}: {res['msg']}[/bold red]")
                    break
    s_conn.close()
    console.print("")
    rpt = Table(title="Migration Report", box=box.SIMPLE)
    rpt.add_column("Tabelle", style="cyan")
    rpt.add_column("Status", style="bold")
    rpt.add_column("Gesamt", justify="right")
    rpt.add_column("Resume", style="dim", justify="right")
    rpt.add_column("Neu", style="green", justify="right")
    rpt.add_column("Skip", style="yellow", justify="right")
    for i in report_data:
        if i["status"]: 
            rpt.add_row(i["name"], "[green]OK[/]", str(i["total"]), str(i["previously_done"]), str(i["inserted"]), str(i["skipped"]))
        else: 
            rpt.add_row(i["name"], "[red]FAIL[/]", "-", "-", "-", "-")
    console.print(rpt)
    if any("statistics" in t for t in plan) and config["timescale_enabled"]:
        if questionary.confirm("TimescaleDB Optimierung jetzt starten?").ask():
            action_convert_schema(config)
    else:
        questionary.press_any_key_to_continue().ask()

def action_migration_menu(config):
    choice = questionary.select("Migrationstyp wählen:", choices=["Alle Tabellen migrieren", "Selektive Auswahl", "Zurück"]).ask()
    if choice == "Zurück":
        return
    target_plan = []
    if choice.startswith("Alle"):
        target_plan = [t['name'] for t in TABLES_CONF]
    else:
        choices = [questionary.Choice(t['name']) for t in TABLES_CONF]
        target_plan = questionary.checkbox("Wähle Tabellen:", choices=choices).ask()
        if not target_plan:
            return
        selected_set = set(target_plan)
        missing = set()
        for t in target_plan:
            if t in DEPENDENCIES:
                for p in DEPENDENCIES[t]:
                    if p not in selected_set:
                        missing.add(p)
        if missing:
            console.print(f"[yellow]Warnung: Fehlende Eltern-Tabellen:[/yellow] {', '.join(missing)}")
            opt = questionary.select("Wie verfahren?", choices=["Fehlende hinzufügen", "Ignorieren", "Abbruch"]).ask()
            if opt == "Abbruch":
                return
            if opt.startswith("Fehlende"):
                 combined = selected_set | missing
                 target_plan = [t['name'] for t in TABLES_CONF if t['name'] in combined]
    run_migration_plan(config, target_plan)

def action_show_stats(config):
    console.print(Panel("[bold]Datenbank Vergleich[/bold]", box=box.ROUNDED))
    s_conn = connect_sqlite_ro(config["sqlite_path"])
    if not s_conn:
        return
    with console.status("Lade Statistiken..."):
        with PostgresManager(config, fast_mode=False) as p_conn:
            p_cur = p_conn.cursor()
            s_cur = s_conn.cursor()
            try:
                s_size = os.path.getsize(config["sqlite_path"])
                p_cur.execute("SELECT pg_database_size(current_database());")
                p_size = p_cur.fetchone()[0]
            except:
                s_size = 0
                p_size = 0
            table = Table(title=f"Größe: SQLite {format_bytes(s_size)} vs Postgres {format_bytes(p_size)}")
            table.add_column("Tabelle", style="cyan")
            table.add_column("Zeilen (SQLite)", justify="right")
            table.add_column("Zeilen (PG)", justify="right")
            table.add_column("Timescale?", justify="center")
            for tbl in TABLES_CONF:
                name = tbl['name']
                try:
                    s_cur.execute(f"SELECT COUNT(*) FROM {name}")
                    s_c = f"{s_cur.fetchone()[0]:,}"
                except Exception as e:
                    s_c = str(e)
                try:
                    p_cur.execute(f"SELECT COUNT(*) FROM {name}")
                    p_c = f"{p_cur.fetchone()[0]:,}"
                    p_cur.execute("SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name=%s", (name,))
                    ts = "✅" if p_cur.fetchone() else "-"
                except:
                    p_c = "err"
                    ts = "?"
                table.add_row(name, s_c, p_c, ts)
    console.print(table)
    s_conn.close()
    questionary.press_any_key_to_continue().ask()

def action_vacuum_menu(config):
    choice = questionary.select("Vacuum-Modus:", choices=["Ganze Datenbank (Full)", "Selektive Tabellen", "Zurück"]).ask()
    if choice == "Zurück":
        return
    if "Full" in choice:
        if questionary.confirm("Vollständiges VACUUM ANALYZE starten? (Kann lange dauern)").ask():
            try:
                conn = psycopg2.connect(host=config["pg_host"], port=config["pg_port"], database=config["pg_db"], user=config["pg_user"], password=config["pg_pass"])
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                with console.status("Führe VACUUM ANALYZE (Full) aus..."):
                    cur.execute("VACUUM ANALYZE;")
                console.print("[green]Fertig.[/green]")
                conn.close()
            except Exception as e:
                console.print(f"[red]Fehler: {e}[/red]")
    else:
        choices = [questionary.Choice(t['name']) for t in TABLES_CONF]
        selected = questionary.checkbox("Tabellen wählen:", choices=choices).ask()
        if selected:
            try:
                conn = psycopg2.connect(host=config["pg_host"], port=config["pg_port"], database=config["pg_db"], user=config["pg_user"], password=config["pg_pass"])
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), TaskProgressColumn()) as progress:
                    task = progress.add_task("Vacuum...", total=len(selected))
                    for t in selected:
                        progress.update(task, description=f"Vacuum {t}...")
                        try:
                            cur.execute(f"VACUUM ANALYZE public.{t};")
                        except Exception as e:
                            console.print(f"[red]Fehler bei {t}: {e}[/red]")
                        progress.advance(task)
                console.print("[green]Fertig.[/green]")
                conn.close()
            except Exception as e:
                console.print(f"[red]Verbindungsfehler: {e}[/red]")
    questionary.press_any_key_to_continue().ask()

def action_sqlite_maintenance(config):
    console.print(Panel("[bold]SQLite Wartung[/bold]", box=box.ROUNDED))
    db_path = config["sqlite_path"]
    
    choice = questionary.select("Aktion wählen:", 
        choices=["VACUUM (Datei neu schreiben - braucht 2x Platz)", "Optimize (WAL Checkpoint + Analyze All)", "Analyze (Einzelne Tabellen)", "Integrity Check", "Zurück"]).ask()
    if choice == "Zurück":
        return

    s_conn = connect_sqlite_rw(db_path)
    if not s_conn:
        return

    try:
        cur = s_conn.cursor()
        if "VACUUM" in choice:
            if questionary.confirm(f"WARNUNG: VACUUM benötigt temporär den doppelten Speicherplatz der DB. Sicher?").ask():
                with console.status("Führe VACUUM durch (Kann lange dauern!)..."):
                    cur.execute("VACUUM;")
                console.print("[green]VACUUM erfolgreich.[/green]")
        elif "Optimize" in choice:
            with console.status("Führe WAL Checkpoint durch..."):
                cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            with console.status("Führe ANALYZE (Full) durch..."):
                cur.execute("ANALYZE;")
            console.print("[green]Optimierung fertig.[/green]")
        elif "Analyze (Einzelne" in choice:
            choices = [questionary.Choice(t['name']) for t in TABLES_CONF]
            selected = questionary.checkbox("Tabellen wählen:", choices=choices).ask()
            if selected:
                with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), TaskProgressColumn()) as progress:
                    task = progress.add_task("Analyze...", total=len(selected))
                    for t in selected:
                        progress.update(task, description=f"Analyze {t}...")
                        try:
                            cur.execute(f"ANALYZE {t};")
                        except Exception as e:
                            console.print(f"[red]Fehler bei {t}: {e}[/red]")
                        progress.advance(task)
                console.print("[green]Fertig.[/green]")
        elif "Integrity" in choice:
            with console.status("Prüfe Integrität..."):
                cur.execute("PRAGMA integrity_check;")
                res = cur.fetchall()
            for r in res:
                if r[0] == "ok":
                    console.print("[green]Integrität OK.[/green]")
                else:
                    console.print(f"[red]Fehler: {r[0]}[/red]")
    except Exception as e:
        console.print(f"[red]Fehler bei SQLite Wartung: {e}[/red]")
    finally:
        s_conn.close()
    questionary.press_any_key_to_continue().ask()

def action_truncate_menu(config):
    console.print(Panel("[bold red]PostgreSQL Tabellen leeren (TRUNCATE)[/bold red]", box=box.ROUNDED))
    console.print("[red]ACHTUNG: Dies löscht ALLE Daten in den gewählten Tabellen![/red]")
    console.print("[red]HINWEIS: Wegen Abhängigkeiten (Foreign Keys) wird CASCADE verwendet.[/red]")
    
    choices = []
    
    s_conn = connect_sqlite_ro(config["sqlite_path"])
    try:
        p_conn = psycopg2.connect(
            host=config["pg_host"], port=config["pg_port"],
            database=config["pg_db"], user=config["pg_user"], password=config["pg_pass"]
        )
        p_cur = p_conn.cursor()
    except:
        p_conn = None
        p_cur = None

    if s_conn:
        s_cur = s_conn.cursor()
        with console.status("Lade aktuelle Tabellengrößen..."):
            for tbl in TABLES_CONF:
                name = tbl['name']
                p_c = "n/a"
                if p_cur:
                    try:
                        p_cur.execute(f"SELECT COUNT(*) FROM public.{name}")
                        p_c = f"{p_cur.fetchone()[0]:,}"
                    except: pass
                s_c = "n/a"
                try:
                    s_cur.execute(f"SELECT COUNT(*) FROM {name}")
                    s_c = f"{s_cur.fetchone()[0]:,}"
                except: pass
                
                label = f"{name:<25} │ PG: {p_c:>10} │ SQ: {s_c:>10}"
                choices.append(questionary.Choice(label, value=name))
        s_conn.close()
    
    if p_conn: p_conn.close()

    if not choices:
        choices = [questionary.Choice(t['name']) for t in TABLES_CONF]

    console.print(f"   {'Tabelle':<25} │ {'Postgres':>10}   │ {'SQLite':>10}")
    console.print(f" {'─'*27}┼{'─'*14}┼{'─'*13}")

    targets = questionary.checkbox("Welche Tabellen leeren?", choices=choices).ask()
    if not targets:
        return
    
    if not questionary.confirm(f"Wirklich {len(targets)} Tabellen unwiderruflich leeren?", default=False).ask():
        return

    with PostgresManager(config, fast_mode=False) as p_conn:
        cur = p_conn.cursor()
        try:
            table_list = ", ".join([f"public.{t}" for t in targets])
            console.print(f"[yellow]Führe TRUNCATE {table_list} CASCADE aus...[/yellow]")
            cur.execute(f"TRUNCATE TABLE {table_list} CASCADE;")
            p_conn.commit()
            console.print("[green]Tabellen erfolgreich geleert.[/green]")
            
            data = load_json(PROGRESS_FILE)
            reset_count = 0
            for t in targets:
                if t in data:
                    del data[t]
                    reset_count += 1
            if reset_count > 0:
                save_json(PROGRESS_FILE, data)
                console.print(f"[green]Fortschritt für {reset_count} Tabellen zurückgesetzt.[/green]")
        except Exception as e:
            p_conn.rollback()
            console.print(f"[bold red]Fehler beim Leeren: {e}[/bold red]")
    questionary.press_any_key_to_continue().ask()

def action_maintenance(config):
    choice = questionary.select("Wartungsoption wählen:", choices=["Sequenzen Reset (Pre-Flight)", "SQLite Optimierung & Check", "Postgres Tabellen leeren (TRUNCATE)", "DB Status Check", "Postgres VACUUM", "Fortschritt zurücksetzen", "Zurück"]).ask()
    if choice.startswith("Sequenzen"):
        action_sequence_reset(config)
    elif choice.startswith("DB Status"):
        action_show_stats(config)
    elif choice.startswith("Fortschritt"):
        action_reset_progress()
    elif choice.startswith("Postgres VACUUM"):
        action_vacuum_menu(config)
    elif choice.startswith("SQLite"):
        action_sqlite_maintenance(config)
    elif choice.startswith("Postgres Tabellen"):
        action_truncate_menu(config)

def main():
    console.clear()
    console.print(Panel.fit("[bold blue]Home Assistant Migration Tool[/bold blue]\n[dim]Next-Gen Edition (v33 - Schema Fix)[/dim]", box=box.DOUBLE))
    if not os.path.exists(CONFIG_FILE):
        if questionary.confirm("Keine Konfiguration gefunden. Jetzt erstellen?").ask():
            action_create_config()
        else:
            return
    try:
        with open(CONFIG_FILE) as f:
            config = json.load(f)
    except:
        console.print("[red]Konfiguration defekt.[/red]")
        return

    while True:
        console.print("")
        choice = questionary.select("Hauptmenü", choices=["Migration starten", "Wartung & Diagnose", "TimescaleDB Optimierung", "Konfiguration bearbeiten", "Beenden"]).ask()
        if choice == "Beenden":
            sys.exit(0)
        elif choice == "Konfiguration bearbeiten":
            action_create_config()
        elif choice == "Wartung & Diagnose":
            action_maintenance(config)
        elif choice == "TimescaleDB Optimierung":
            action_convert_schema(config)
        elif choice == "Migration starten":
            action_migration_menu(config)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[yellow]Beendet durch Benutzer.[/yellow]")