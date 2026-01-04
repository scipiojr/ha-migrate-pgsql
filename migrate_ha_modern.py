"""
Home Assistant Migration Tool (SQLite to PostgreSQL)
Version: 69 (Production Grade + Adaptive Batching + Verification)
Author: Gemini (AI)
License: MIT

DESCRIPTION:
This tool migrates data from SQLite to PostgreSQL with a focus on data integrity.
It handles quirks like Foreign Key dependencies and mixed data types.

KEY FEATURES:
1. Global Layering: Migrates metadata first to satisfy Foreign Keys.
2. Fast Mode: Temporarily disables constraints for speed (if possible).
3. Live-Start: Sets sequences early so HA can run during history import.
4. Smart Repair: Dual-Mode repair center for SQLite and Postgres.
5. Auto-Healing: Adaptive Batch Size reduces automatically on connection drops.
6. Verification: Post-migration audit comparing row counts.
"""

import sqlite3
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from dateutil import parser as date_parser
from datetime import datetime, timezone
import sys
import os
import json
import time
import re

# UI Libraries
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn
)
from rich import box
import questionary
from questionary import Choice

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================

DEFAULT_CONFIG = {
    "sqlite_path": "home-assistant_v2.db",
    "pg_host": "localhost",
    "pg_port": "5432",
    "pg_db": "homeassistant",
    "pg_user": "homeassistant",
    "pg_pass": "",
    "batch_size": 10000,  # Starting Value (will adapt automatically)
    "seq_buffer": 50000,
    "timescale_enabled": False
}

CONFIG_FILE = "config.json"
PROGRESS_FILE = "migration_progress.json"
ERROR_LOG_FILE = "migration_errors.log"
SKIP_LOG_FILE = "migration_skipped.log"

console = Console()

# ==============================================================================
# SCHEMA DEFINITIONS
# ==============================================================================

TABLES_CONF = [
    # --- PHASE 1: Metadata (Level 0-2) ---
    {"name": "schema_changes", "pk": "change_id"},
    {"name": "event_data", "pk": "data_id"},
    {"name": "event_types", "pk": "event_type_id"},
    {"name": "states_meta", "pk": "metadata_id"},
    {"name": "state_attributes", "pk": "attributes_id"},
    {"name": "statistics_meta", "pk": "id"},
    {"name": "recorder_runs", "pk": "run_id", "time_col": "start_ts"},
    {"name": "statistics_runs", "pk": "run_id", "time_col": "start_ts"},

    # --- PHASE 2: Payload (Level 3) ---
    {"name": "events", "pk": "event_id", "time_col": "time_fired_ts"},
    {"name": "states", "pk": "state_id", "time_col": "last_updated_ts", "drop_fk": ["states_old_state_id_fkey"]},
    {"name": "statistics", "pk": "id", "time_col": "start_ts", "segment_by": "metadata_id"},
    {"name": "statistics_short_term", "pk": "id", "time_col": "start_ts", "segment_by": "metadata_id"},
]

# Identify Phase 2 tables for the split logic
PHASE_2_TABLE_NAMES = ["events", "states", "statistics", "statistics_short_term"]

TABLE_MAP = {t["name"]: t for t in TABLES_CONF}

# ==============================================================================
# IO & UTILITIES
# ==============================================================================

def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                return json.load(f) or {}
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
            f.write(f"SQL QUERY: {sql_query[:500]}...\n")
            f.write(f"{'-'*80}\n")
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
            f.write(f"{'-'*40}\n")
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

def format_ts(ts):
    if not ts:
        return "-"
    try:
        return datetime.fromtimestamp(ts).strftime("%d.%m.%y %H:%M")
    except:
        return str(ts)

# ==============================================================================
# DB CONNECTION MANAGERS
# ==============================================================================

def sqlite_regexp(expr, item):
    """Python-based REGEXP function for SQLite."""
    try:
        reg = re.compile(expr)
        return reg.search(item) is not None
    except Exception:
        return False

class PostgresManager:
    """Context manager for Postgres with Keepalive and Fast-Mode support."""
    def __init__(self, config, fast_mode=True):
        self.cfg = config
        self.fast_mode = fast_mode
        self.conn = None
        self.fast_mode_active = False

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                host=self.cfg["pg_host"],
                port=self.cfg["pg_port"],
                database=self.cfg["pg_db"],
                user=self.cfg["pg_user"],
                password=self.cfg["pg_pass"],
                application_name="ha_migrator_v69",
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            with self.conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC';")
                if self.fast_mode:
                    try:
                        cur.execute("SET session_replication_role = 'replica';")
                        self.fast_mode_active = True
                    except psycopg2.errors.InsufficientPrivilege:
                        self.conn.rollback()
                        console.print("[yellow]WARNUNG: Keine Superuser-Rechte. Safe Mode aktiv.[/yellow]")
                        self.fast_mode_active = False
                    except Exception as e:
                        self.conn.rollback()
                        console.print(f"[red]Warnung: Konnte Fast-Mode nicht aktivieren: {e}[/red]")
                        self.fast_mode_active = False
            self.conn.commit()
            return self.conn
        except Exception as e:
            console.print(f"[bold red]FATAL: Postgres Verbindung fehlgeschlagen:[/bold red] {e}")
            sys.exit(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if self.fast_mode and self.fast_mode_active:
                    with self.conn.cursor() as cur:
                        cur.execute("SET session_replication_role = 'origin';")
                    self.conn.commit()
            except:
                pass
            try:
                self.conn.close()
            except:
                pass

def connect_sqlite_ro(path):
    """Connect to SQLite in Read-Only mode."""
    if not os.path.exists(path):
        console.print(f"[bold red]FATAL: SQLite Datei fehlt:[/bold red] {path}")
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=300)
        conn.row_factory = sqlite3.Row
        return conn
    except:
        conn = sqlite3.connect(path, timeout=300)
        conn.row_factory = sqlite3.Row
        return conn

def connect_sqlite_rw(path):
    """Connect to SQLite in Read-Write mode (Maintenance)."""
    if not os.path.exists(path):
        console.print(f"[bold red]FATAL: SQLite Datei fehlt:[/bold red] {path}")
        return None
    try:
        conn = sqlite3.connect(path, timeout=600)
        # Register REGEXP for repair functions
        conn.create_function("REGEXP", 2, sqlite_regexp)
        return conn
    except Exception as e:
        console.print(f"[red]Fehler beim Öffnen von SQLite (RW): {e}[/red]")
        return None

# ==============================================================================
# DB HELPERS & ANALYTICS
# ==============================================================================

def check_postgres_schema_exists(config):
    """Verifies that target tables actually exist in Postgres."""
    with PostgresManager(config, fast_mode=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables = {row[0] for row in cur.fetchall()}
    
    missing = []
    for t in TABLES_CONF:
        if t['name'] not in tables:
            missing.append(t['name'])
    
    if missing:
        console.print(Panel(
            "[bold red]CRITICAL: Ziel-Datenbank ist leer![/bold red]\n\n"
            "PostgreSQL enthält noch keine Tabellen.\n"
            "1. Starte Home Assistant kurz gegen diese DB.\n"
            "2. Stoppe HA.\n"
            "3. Starte dieses Skript erneut.",
            title="Fehler: Schema fehlt", box=box.HEAVY, style="red"
        ))
        return False
    return True

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
            
            p_cur.execute(f"SELECT MIN({pk_col}) FROM public.{table_name} WHERE {pk_col} > %s", (s_max,))
            stats["barrier"] = p_cur.fetchone()[0]
            
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

# ==============================================================================
# TYPE CONVERTERS
# ==============================================================================

def _conv_noop(val):
    return val

def _conv_bool(val):
    if val is None:
        return None
    if isinstance(val, int):
        return bool(val)
    if isinstance(val, str):
        return val.lower() in ('true', '1', 't', 'y', 'yes')
    return val

def _conv_timestamp(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val, tz=timezone.utc)
    if isinstance(val, str):
        try:
            dt = date_parser.parse(val)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except:
            return None
    return None

def _conv_int(val):
    if val is None:
        return None
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        try:
            return int(float(val))
        except:
            return None
    return val

def _conv_str_clean(val):
    if val is None:
        return None
    s_val = str(val)
    if '\0' in s_val:
        return s_val.replace('\0', '')
    return s_val

def get_converter_for_type(target_type):
    tt = target_type.lower()
    if 'boolean' in tt:
        return _conv_bool
    if 'timestamp' in tt or 'date' in tt:
        return _conv_timestamp
    if 'int' in tt or 'serial' in tt:
        return _conv_int
    if 'char' in tt or 'text' in tt:
        return _conv_str_clean
    return _conv_noop

# ==============================================================================
# MIGRATION LOGIC (WITH ADAPTIVE BATCHING)
# ==============================================================================

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

def migrate_single_table(s_conn, p_conn, tbl_conf, cfg, progress_map, rich_progress, task_id, current_batch_size):
    """
    Migrates a single table using batch processing.
    Returns dictionary with status and specific error messages for adaptive handling.
    """
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
        return {"status": False, "msg": "Keine gemeinsamen Spalten"}

    converters = [get_converter_for_type(col_types[c]) for c in common_cols]

    with p_conn.cursor() as check_cur:
        constraints = get_blocking_constraints(check_cur, table_name)
    if constraints:
        # Just logging for info
        pass

    quoted_cols = [f'"{c}"' for c in common_cols]
    conflict_sql = "ON CONFLICT DO NOTHING"
    insert_sql = f"INSERT INTO public.{table_name} ({', '.join(quoted_cols)}) VALUES %s {conflict_sql}"
    
    if pk_col:
        insert_sql += f" RETURNING {pk_col}"

    saved_start_id = progress_map.get(table_name, 0)
    s_cur = s_conn.cursor()
    rich_progress.update(task_id, description=f"[cyan]Analysiere {table_name}...", total=None)
    
    total_rows_abs = 0
    total_rows_todo = 0
    max_src_id = 0
    final_start_id = saved_start_id
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

    rich_progress.update(task_id, description=f"[bold cyan]{table_name} (Batch: {current_batch_size})", total=total_rows_todo, completed=0)

    # Initial sequence update (only if not a phase 2 table being run in phase 1 context)
    if pk_col and table_name not in PHASE_2_TABLE_NAMES:
        update_sequence_if_needed(p_cur, table_name, pk_col, max_src_id, cfg['seq_buffer'])
        p_conn.commit()

    curr_off = final_start_id
    stats_inserted = 0
    stats_skipped = 0
    
    pk_idx = -1
    if pk_col:
        try:
            pk_idx = common_cols.index(pk_col)
        except:
            pk_idx = -1

    try:
        while True:
            # Batch Fetch
            if pk_col:
                s_cur.execute(f"SELECT {', '.join(quoted_cols)} FROM {table_name} WHERE {pk_col} > ? AND {pk_col} <= ? ORDER BY {pk_col} ASC LIMIT ?", (curr_off, max_src_id, current_batch_size))
            else:
                s_cur.execute(f"SELECT {', '.join(quoted_cols)} FROM {table_name} LIMIT ? OFFSET ?", (current_batch_size, curr_off))

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

            # Batch Insert
            if pk_col:
                try:
                    returned_rows = execute_values(p_cur, insert_sql, batch_data, page_size=current_batch_size, fetch=True)
                    inserted_ids_set = set(r[0] for r in returned_rows)
                    batch_inserted = len(inserted_ids_set)
                except Exception as e:
                    err_s = str(e)
                    if "server closed the connection" in err_s or "OperationalError" in err_s or "closed unexpectedly" in err_s:
                        # CRITICAL: Re-raise specifically for adaptive handling
                        raise ConnectionError("POSTGRES_DROP")
                    
                    # FALLBACK: If batch fails logic wise (not connection), try fallback
                    insert_sql_fallback = insert_sql.replace(f" RETURNING {pk_col}", "")
                    execute_values(p_cur, insert_sql_fallback, batch_data, page_size=current_batch_size)
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
                # No PK
                execute_values(p_cur, insert_sql, batch_data, page_size=current_batch_size)
                batch_inserted = p_cur.rowcount
                if batch_inserted < 0:
                    batch_inserted = 0
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

        # Final sequence check for non-phase-2 tables
        if pk_col and table_name not in PHASE_2_TABLE_NAMES:
            update_sequence_if_needed(p_cur, table_name, pk_col, max_src_id, cfg['seq_buffer'])
            p_conn.commit()

        return {"status": True, "total": total_rows_abs, "inserted": stats_inserted, "skipped": stats_skipped, "previously_done": already_done_count}

    except ConnectionError as ce:
        # Pass this up to trigger adaptive batching
        return {"status": False, "msg": "CONNECTION_DROP"}
    except Exception as e:
        try:
            p_conn.rollback()
        except:
            pass
        
        # Check again if it was a connection drop hidden in generic exception
        if "closed" in str(e) or "OperationalError" in str(e):
             return {"status": False, "msg": "CONNECTION_DROP"}

        log_error_to_file(table_name, str(e), insert_sql, batch_data)
        return {"status": False, "msg": str(e)}
    finally:
        try:
            p_cur.close()
            s_cur.close()
        except:
            pass

def run_phase_with_retry(config, plan_tables, s_conn, progress_data, report_data, phase_name):
    """
    Executes migration for a list of tables with ADAPTIVE BATCH SIZING.
    If connection drops, it reconnects, halves batch size, and resumes.
    """
    if not plan_tables:
        return

    console.print(Panel(f"[bold]{phase_name}[/bold]", style="blue"))
    
    # We use a queue-like approach
    pending_tables = list(plan_tables)
    current_batch_size = config["batch_size"]
    
    with Progress(SpinnerColumn(), TextColumn("[bold blue]{task.description}"), BarColumn(), TaskProgressColumn(), TextColumn("•"), TimeRemainingColumn()) as rich_progress:
        while pending_tables:
            t_name = pending_tables[0]
            task_id = rich_progress.add_task(f"Warte auf {t_name}...", start=False)
            rich_progress.start_task(task_id)
            
            # Retry loop for CURRENT table
            while True:
                try:
                    # New Connection Scope per Attempt
                    with PostgresManager(config, fast_mode=True) as p_conn:
                        res = migrate_single_table(
                            s_conn, p_conn, TABLE_MAP[t_name], config, 
                            progress_data, rich_progress, task_id, current_batch_size
                        )
                    
                    if res["status"]:
                        # Success
                        res["name"] = t_name
                        report_data.append(res)
                        pending_tables.pop(0) # Done
                        break # Next table
                    
                    elif res["msg"] == "CONNECTION_DROP":
                        # Auto-Healing Logic
                        new_batch = int(current_batch_size / 2)
                        if new_batch < 100:
                            console.print(f"[bold red]FATAL: Batch-Größe zu klein ({new_batch}). Netzwerk instabil.[/bold red]")
                            return # Abort
                        
                        console.print(f"[yellow]⚠ Verbindung verloren! Reduziere Batch von {current_batch_size} auf {new_batch} und versuche Resume...[/yellow]")
                        current_batch_size = new_batch
                        # Wait a bit before reconnect
                        time.sleep(2)
                        # Loop continues -> Reconnects -> Resumes via progress_map
                        
                    else:
                        # Real Error
                        console.print(f"[bold red]Abbruch bei {t_name}: {res['msg']}[/bold red]")
                        res["name"] = t_name
                        report_data.append(res)
                        return # Abort entire migration

                except Exception as e:
                    console.print(f"[red]Unerwarteter Fehler im Retry-Loop: {e}[/red]")
                    return

def run_migration_plan(config, plan):
    """Orchestrates the migration of multiple tables."""
    if not check_postgres_schema_exists(config):
        return

    s_conn = connect_sqlite_ro(config["sqlite_path"])
    if not s_conn:
        return
    with PostgresManager(config, fast_mode=False) as p_conn:
        run_preflight_analysis(s_conn, p_conn, plan)
    
    # SPLIT PLAN
    phase1_plan = [t for t in plan if t not in PHASE_2_TABLE_NAMES]
    phase2_plan = [t for t in plan if t in PHASE_2_TABLE_NAMES]
    
    report_data = []
    progress_data = load_json(PROGRESS_FILE)

    # --- PHASE 1 ---
    run_phase_with_retry(config, phase1_plan, s_conn, progress_data, report_data, "Phase 1: Metadaten")

    # --- INTERMISSION: LIVE START ---
    if phase2_plan:
        console.print("")
        console.print(Panel(
            "[bold green]PHASE 1 ABGESCHLOSSEN![/bold green]\n\n"
            "Das Skript wird nun die Datenbank-Sequenzen hochsetzen.\n"
            "Danach können Sie [bold]Home Assistant starten[/bold].\n"
            "HA wird neue Daten schreiben, während dieses Skript die Historie (Phase 2) im Hintergrund auffüllt.",
            title="Ready for Live-Start", box=box.DOUBLE, style="green"
        ))
        
        with console.status("Bereite Live-Start vor (Sequenzen update)..."):
            with PostgresManager(config, fast_mode=False) as p_conn:
                p_cur = p_conn.cursor()
                s_cur = s_conn.cursor()
                for t_name in phase2_plan:
                    tbl = TABLE_MAP[t_name]
                    if tbl.get('pk'):
                        max_sq = get_max_id(s_cur, t_name, tbl['pk'])
                        update_sequence_if_needed(p_cur, t_name, tbl['pk'], max_sq, config['seq_buffer'])
                p_conn.commit()
                p_cur.close()
                s_cur.close()
        
        questionary.press_any_key_to_continue("Drücken Sie eine Taste, um Phase 2 (Historie) zu starten...").ask()

    # --- PHASE 2 ---
    run_phase_with_retry(config, phase2_plan, s_conn, progress_data, report_data, "Phase 2: Historie (Backfill)")

    s_conn.close()
    
    # Final Report
    console.print("")
    rpt = Table(title="Migration Report", box=box.SIMPLE)
    rpt.add_column("Tabelle", style="cyan"); rpt.add_column("Status", style="bold"); rpt.add_column("Gesamt", justify="right")
    rpt.add_column("Resume", style="dim", justify="right"); rpt.add_column("Neu", style="green", justify="right"); rpt.add_column("Skip", style="yellow", justify="right")
    for i in report_data:
        if i["status"]: 
            rpt.add_row(i["name"], "[green]OK[/]", str(i["total"]), str(i["previously_done"]), str(i["inserted"]), str(i["skipped"]))
        else: 
            rpt.add_row(i["name"], "[red]FAIL[/]", "-", "-", "-", "-")
    console.print(rpt)
    
    # Auto Verify
    action_verify_migration(config)
    
    if any("statistics" in t for t in plan) and config.get("timescale_enabled", False):
        if questionary.confirm("TimescaleDB Optimierung jetzt starten?").ask():
            action_convert_schema(config)
    else:
        questionary.press_any_key_to_continue().ask()

# ==============================================================================
# REPAIR CENTER (SMART MERGE) - DUAL DB SUPPORT
# ==============================================================================

class DBRepairContext:
    """Wrapper to handle different DB backends (SQLite vs Postgres) uniformly."""
    def __init__(self, config, db_type):
        self.config = config
        self.db_type = db_type
        self.conn = None
        self.pg_mgr = None
        
    def __enter__(self):
        if self.db_type == "postgres":
            self.pg_mgr = PostgresManager(self.config, fast_mode=False)
            self.conn = self.pg_mgr.__enter__()
        else:
            self.conn = connect_sqlite_rw(self.config["sqlite_path"])
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_type == "postgres":
            self.pg_mgr.__exit__(exc_type, exc_val, exc_tb)
        elif self.conn:
            self.conn.close()

def analyze_and_fix_interactive(config, title, merge_mode=True, repair_type="duplicates", db_type="postgres"):
    """
    Generic Repair Function: Finds conflicts, analyzes stats, and offers merge options.
    Adaptable for both PostgreSQL and SQLite.
    """
    
    # 1. Prepare Query based on DB Type and Repair Type
    search_sql = ""
    if db_type == "postgres":
        if repair_type == "duplicates":
            search_sql = """
                SELECT statistic_id, jsonb_agg(id ORDER BY id ASC) as ids 
                FROM statistics_meta 
                GROUP BY statistic_id 
                HAVING count(*) > 1;
            """
        elif repair_type == "suffixes":
            search_sql = r"""
                SELECT 
                    m1.statistic_id, 
                    jsonb_build_array(m1.id) || jsonb_agg(m2.id)
                FROM statistics_meta m1
                JOIN statistics_meta m2 
                    ON m2.statistic_id LIKE m1.statistic_id || '%\_%'
                    AND m2.statistic_id ~ ('^' || replace(m1.statistic_id, '.', '\.') || '_\d+$')
                GROUP BY m1.statistic_id, m1.id
            """
    else: # SQLite
        if repair_type == "duplicates":
            search_sql = """
                SELECT statistic_id, json_group_array(id)
                FROM statistics_meta
                GROUP BY statistic_id
                HAVING count(*) > 1;
            """
        elif repair_type == "suffixes":
            search_sql = r"""
                SELECT 
                    m1.statistic_id, 
                    json_group_array(m2.id)
                FROM statistics_meta m1
                JOIN statistics_meta m2 
                    ON m2.statistic_id REGEXP ('^' || REPLACE(m1.statistic_id, '.', '\.') || '_\d+$')
                    OR m2.id = m1.id
                GROUP BY m1.statistic_id
                HAVING count(*) > 1
            """

    with DBRepairContext(config, db_type) as conn:
        cur = conn.cursor()
        console.print(f"[cyan]{title} ({db_type.upper()})...[/cyan]")
        
        try:
            cur.execute(search_sql)
            candidates = cur.fetchall()
        except Exception as e:
            console.print(f"[red]Fehler bei der Suche: {e}[/red]")
            return

        if not candidates:
            console.print("[green]Keine Probleme gefunden.[/green]")
            return

        conflicts = []
        with console.status("Analysiere Daten-Historie..."):
            for row in candidates:
                if merge_mode:
                    if db_type == "postgres":
                        if len(row) == 2:
                            name = row[0]
                            ids = row[1]
                        else:
                            continue
                    else:
                        name = row[0]
                        try:
                            ids = json.loads(row[1])
                        except:
                            continue

                    ids = sorted(list(set(ids)))

                    stats = []
                    for eid in ids:
                        try:
                            q_lts = "SELECT COUNT(*), MIN(start_ts), MAX(start_ts) FROM statistics WHERE metadata_id = %s" if db_type == 'postgres' else "SELECT COUNT(*), MIN(start_ts), MAX(start_ts) FROM statistics WHERE metadata_id = ?"
                            cur.execute(q_lts, (eid,))
                            res_lts = cur.fetchone()
                            
                            q_st = "SELECT COUNT(*) FROM statistics_short_term WHERE metadata_id = %s" if db_type == 'postgres' else "SELECT COUNT(*) FROM statistics_short_term WHERE metadata_id = ?"
                            cur.execute(q_st, (eid,))
                            cnt_st = cur.fetchone()[0]
                            
                            rows = (res_lts[0] or 0) + (cnt_st or 0)
                            min_ts = res_lts[1]
                            max_ts = res_lts[2] or 0
                            
                            stats.append({
                                "id": eid,
                                "rows": rows,
                                "min_ts": min_ts,
                                "max_ts": max_ts
                            })
                        except Exception:
                            stats.append({"id": eid, "rows": 0, "min_ts": 0, "max_ts": 0})
                    
                    stats.sort(key=lambda x: x["max_ts"], reverse=True)
                    
                    if not stats: continue

                    target = stats[0]
                    sources = stats[1:]
                    source_ids = [s["id"] for s in sources]
                    
                    conflicts.append({
                        "name": name,
                        "target_id": target["id"],
                        "source_ids": source_ids,
                        "target_stats": f"ID {target['id']}: {target['rows']} Zeilen ({format_ts(target['min_ts'])} - {format_ts(target['max_ts'])})",
                        "source_stats": "\n".join([f"ID {s['id']}: {s['rows']} Zeilen ({format_ts(s['min_ts'])} - {format_ts(s['max_ts'])})" for s in sources])
                    })

        table = Table(title=f"Gefundene Konflikte: {len(conflicts)}", show_lines=True)
        table.add_column("Sensor", style="cyan")
        table.add_column("Merge", style="bold magenta")
        table.add_column("Details (Ziel)", style="green")
        table.add_column("Details (Quelle/Alt)", style="yellow")
        
        for c in conflicts:
            table.add_row(
                c["name"],
                f"{c['source_ids']} -> {c['target_id']}",
                c["target_stats"],
                c["source_stats"]
            )
        console.print(table)
        
        if not questionary.confirm("Möchtest du diese Konflikte bearbeiten?").ask():
            return

        with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), TaskProgressColumn()) as progress:
            task = progress.add_task("Bearbeite...", total=len(conflicts))
            
            for c in conflicts:
                progress.stop() 
                console.print(f"\n[bold]{c['name']}[/bold] (Merge {c['source_ids']} -> {c['target_id']})")
                
                action = questionary.select(
                    "Aktion:",
                    choices=[
                        Choice("Merge durchführen", value="merge"),
                        Choice("Ziel-ID ändern", value="edit"),
                        Choice("Skip", value="skip")
                    ]
                ).ask()
                
                progress.start()
                
                target_id = c["target_id"]
                
                if action == "skip":
                    progress.advance(task)
                    continue
                
                if action == "edit":
                    target_id = int(questionary.text("Neue Ziel-ID:", default=str(target_id)).ask())
                
                try:
                    if db_type == "postgres":
                        cur.execute(f"UPDATE statistics SET metadata_id = {target_id} WHERE metadata_id = ANY(%s)", (c['source_ids'],))
                        cur.execute(f"UPDATE statistics_short_term SET metadata_id = {target_id} WHERE metadata_id = ANY(%s)", (c['source_ids'],))
                        cur.execute(f"DELETE FROM statistics_meta WHERE id = ANY(%s)", (c['source_ids'],))
                    else: # SQLite
                        placeholders = ','.join('?' * len(c['source_ids']))
                        cur.execute(f"UPDATE statistics SET metadata_id = ? WHERE metadata_id IN ({placeholders})", [target_id] + c['source_ids'])
                        cur.execute(f"UPDATE statistics_short_term SET metadata_id = ? WHERE metadata_id IN ({placeholders})", [target_id] + c['source_ids'])
                        cur.execute(f"DELETE FROM statistics_meta WHERE id IN ({placeholders})", c['source_ids'])

                    conn.commit()
                    console.print(f"[green]✓ {c['name']} gemerged.[/green]")
                except Exception as e:
                    conn.rollback()
                    console.print(f"[red]✗ Fehler: {e}[/red]")
                
                progress.advance(task)

def fix_duplicates_smart(config, db_type):
    analyze_and_fix_interactive(config, "Suche exakte Duplikate", merge_mode=True, repair_type="duplicates", db_type=db_type)

def fix_suffixes_smart(config, db_type):
    analyze_and_fix_interactive(config, "Suche Suffixe (_2, _3...)", merge_mode=True, repair_type="suffixes", db_type=db_type)

def fix_missing_units_interactive(config):
    with PostgresManager(config, fast_mode=False) as p_conn:
        cur = p_conn.cursor()
        console.print("[cyan]Suche nach fehlenden Einheiten (NULL) [Postgres]...[/cyan]")
        
        rules = [
            ("%_energy%", "kWh"), ("%_consumption%", "kWh"), ("%_power%", "W"),
            ("%_temperature%", "°C"), ("%_humidity%", "%"), ("%_battery%", "%"),
            ("%_voltage%", "V"), ("%_current%", "A")
        ]
        
        candidates = []
        for pattern, unit in rules:
            cur.execute("SELECT id, statistic_id FROM statistics_meta WHERE unit_of_measurement IS NULL AND statistic_id LIKE %s", (pattern,))
            for r in cur.fetchall():
                candidates.append({"id": r[0], "name": r[1], "new_unit": unit})
        
        if not candidates:
            console.print("[green]Keine fehlenden Einheiten gefunden.[/green]")
            return

        choices = [Choice(f"{c['name']} -> Setze '{c['new_unit']}'", value=c, checked=True) for c in candidates]
        selected = questionary.checkbox("Wähle Einheiten:", choices=choices).ask()
        
        if not selected:
            return

        action = questionary.select(
            f"{len(selected)} Einträge ausgewählt. Wie verfahren?",
            choices=["Direkt anwenden", "Einzeln prüfen", "Abbrechen"]
        ).ask()

        if action == "Abbrechen":
            return

        final_list = selected
        if "Einzeln" in action:
            final_list = []
            for item in selected:
                console.print(f"\n[bold]{item['name']}[/bold]")
                d = questionary.select(f"Vorschlag: {item['new_unit']}", choices=[Choice("OK", value="accept"), Choice("Edit", value="edit"), Choice("Skip", value="skip")]).ask()
                if d == "skip": continue
                if d == "edit": item['new_unit'] = questionary.text("Einheit:", default=item['new_unit']).ask()
                final_list.append(item)

        with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), TaskProgressColumn()) as progress:
            task = progress.add_task("Update...", total=len(final_list))
            for c in final_list:
                progress.update(task, description=f"Fixing {c['name']}...")
                try:
                    cur.execute("UPDATE statistics_meta SET unit_of_measurement = %s WHERE id = %s", (c['new_unit'], c['id']))
                    p_conn.commit()
                except:
                    p_conn.rollback()
                progress.advance(task)
        console.print("[green]Fertig.[/green]")

def action_fix_metadata(config):
    console.print(Panel("[bold]Repair Center: Statistik & Metadaten (Smart)[/bold]", box=box.ROUNDED))
    db_target = questionary.select("Datenbank-Ziel:", choices=[
        Choice("PostgreSQL (Ziel)", value="postgres"),
        Choice("SQLite (Quelle)", value="sqlite"),
        "Zurück"
    ]).ask()
    
    if db_target == "Zurück":
        return

    while True:
        mode = questionary.select(f"Kategorie wählen ({db_target.upper()}):", choices=["Exakte Duplikate (Smart Merge)", "Suffixe (_2, _3...) (Smart Merge)", "Fehlende Einheiten (Units, PG only)", "Zurück"]).ask()
        if mode == "Zurück":
            return
        if "Duplikate" in mode:
            fix_duplicates_smart(config, db_target)
        elif "Suffixe" in mode:
            fix_suffixes_smart(config, db_target)
        elif "Einheiten" in mode:
            if db_target == "sqlite":
                console.print("[yellow]Unit-Fixing currently implemented for Postgres only.[/yellow]")
            else:
                fix_missing_units_interactive(config)
        console.print("")

# ==============================================================================
# ACTIONS & MENUS
# ==============================================================================

def action_verify_migration(config):
    """Checks row counts between SQLite and Postgres."""
    console.print(Panel("[bold]Post-Migration Audit (Verifikation)[/bold]", box=box.ROUNDED))
    
    s_conn = connect_sqlite_ro(config["sqlite_path"])
    if not s_conn: return

    with console.status("Vergleiche Datenbanken..."):
        with PostgresManager(config, fast_mode=False) as p_conn:
            p_cur = p_conn.cursor()
            s_cur = s_conn.cursor()
            
            table = Table(box=box.SIMPLE)
            table.add_column("Tabelle", style="cyan")
            table.add_column("SQLite", justify="right")
            table.add_column("Postgres", justify="right")
            table.add_column("Diff", justify="right", style="bold")
            
            for tbl in TABLES_CONF:
                name = tbl['name']
                try:
                    s_cur.execute(f"SELECT COUNT(*) FROM {name}")
                    s_c = s_cur.fetchone()[0]
                except: s_c = 0
                
                try:
                    p_cur.execute(f"SELECT COUNT(*) FROM public.{name}")
                    p_c = p_cur.fetchone()[0]
                except: p_c = 0
                
                diff = p_c - s_c
                diff_str = f"[green]{diff:+}[/green]" if diff >= 0 else f"[red]{diff:+}[/red]"
                
                if diff < 0:
                    diff_str += " ⚠️"
                
                table.add_row(name, f"{s_c:,}", f"{p_c:,}", diff_str)
                
            p_cur.close(); s_cur.close()
    
    s_conn.close()
    console.print(table)
    console.print("[dim]Hinweis: Positive Differenz bei Postgres ist normal, wenn Home Assistant bereits läuft.[/dim]\n")
    questionary.press_any_key_to_continue().ask()

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
    return config

def action_reset_progress():
    data = load_json(PROGRESS_FILE)
    if not data:
        console.print("[yellow]Keine Daten.[/yellow]")
        questionary.press_any_key_to_continue().ask()
        return
    choice = questionary.select("Was zurücksetzen?", choices=["Alles", "Einzeln", "Abbrechen"]).ask()
    if choice == "Abbrechen":
        return
    if choice == "Alles":
        os.remove(PROGRESS_FILE)
        console.print("[green]Alles zurückgesetzt.[/green]")
    elif choice == "Einzeln":
        sel = questionary.checkbox("Tabellen:", choices=[questionary.Choice(k) for k in data.keys()]).ask()
        if sel:
            for t in sel:
                del data[t]
            save_json(PROGRESS_FILE, data)
            console.print(f"[green]{len(sel)} Tabellen zurückgesetzt.[/green]")
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
                table.add_row(name, f"UPDATE: {curr} -> {target}" if updated else f"OK ({curr} > {target})")
            p_cur.close()
    console.print(table)
    s_conn.close()
    questionary.press_any_key_to_continue().ask()

def action_convert_schema(config):
    if not config.get("timescale_enabled", False):
        console.print("[yellow]Hinweis: TimescaleDB ist deaktiviert.[/yellow]")
        if not questionary.confirm("Trotzdem versuchen?").ask():
            return
    console.print(Panel("[bold]TimescaleDB Konvertierung[/bold]", box=box.ROUNDED))
    targets = questionary.checkbox("Wähle Tabellen:", choices=[questionary.Choice(t['name'], value=t) for t in TABLES_CONF if "time_col" in t]).ask()
    if not targets:
        return
    if any(t['name'] == 'states' for t in targets):
        if not questionary.confirm("[bold yellow]WARNUNG: 'states' Foreign Keys werden gelöscht. Fortfahren?[/bold yellow]").ask():
            return
    with PostgresManager(config, fast_mode=False) as p_conn:
        c = p_conn.cursor()
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), transient=True) as progress:
            task = progress.add_task("Verarbeite...", total=len(targets))
            for tbl in targets:
                name = tbl['name']
                time_col = tbl['time_col']
                progress.update(task, description=f"Prüfe {name}...")
                try:
                    c.execute(f"DELETE FROM {name} WHERE {time_col} IS NULL;") # Clean NULLs
                    c.execute("SELECT data_type FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (name, time_col))
                    if "double" in c.fetchone()[0].lower():
                        console.print(f"[red]Skip {name}: Column {time_col} is FLOAT (Needs TIMESTAMP).[/red]")
                        progress.advance(task)
                        continue
                    # Conversion Logic
                    if "drop_fk" in tbl:
                        for fk in tbl["drop_fk"]:
                            c.execute(f"ALTER TABLE {name} DROP CONSTRAINT IF EXISTS {fk};")
                    c.execute(f"ALTER TABLE {name} DROP CONSTRAINT {name}_pkey CASCADE;")
                    c.execute(f"ALTER TABLE {name} ADD PRIMARY KEY ({tbl['pk']}, {tbl['time_col']});")
                    c.execute(f"SELECT create_hypertable('{name}', '{time_col}', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, migrate_data => TRUE);")
                    c.execute(f"ALTER TABLE {name} SET (timescaledb.compress);")
                    c.execute(f"SELECT add_compression_policy('{name}', INTERVAL '14 days');")
                    p_conn.commit()
                    console.print(f"[green]✓ {name} konvertiert[/green]")
                except Exception as e:
                    p_conn.rollback()
                    console.print(f"[red]✗ {name}: {e}[/red]")
                progress.advance(task)
    questionary.press_any_key_to_continue().ask()

def action_show_stats(config):
    action_verify_migration(config)

def action_vacuum_menu(config):
    choice = questionary.select("Vacuum-Modus:", choices=["Full", "Selektiv", "Zurück"]).ask()
    if choice == "Zurück":
        return
    conn = psycopg2.connect(host=config["pg_host"], port=config["pg_port"], database=config["pg_db"], user=config["pg_user"], password=config["pg_pass"])
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    if choice == "Full":
        if questionary.confirm("Starten?").ask():
            with console.status("Running VACUUM FULL..."):
                cur.execute("VACUUM ANALYZE;")
    else:
        sel = questionary.checkbox("Tabellen:", choices=[questionary.Choice(t['name']) for t in TABLES_CONF]).ask()
        if sel:
            for t in sel: 
                with console.status(f"Vacuum {t}..."):
                    cur.execute(f"VACUUM ANALYZE public.{t};")
    conn.close()
    console.print("[green]Fertig.[/green]")
    questionary.press_any_key_to_continue().ask()

def action_truncate_menu(config):
    targets = questionary.checkbox("Tabellen leeren (TRUNCATE):", choices=[questionary.Choice(t['name']) for t in TABLES_CONF]).ask()
    if not targets or not questionary.confirm(f"WARNUNG: Lösche {len(targets)} Tabellen unwiderruflich. Sicher?", default=False).ask():
        return
    with PostgresManager(config, fast_mode=False) as p_conn:
        cur = p_conn.cursor()
        try:
            cur.execute(f"TRUNCATE TABLE {', '.join([f'public.{t}' for t in targets])} CASCADE;")
            p_conn.commit()
            console.print("[green]Geleert.[/green]")
            # Cleanup Progress
            data = load_json(PROGRESS_FILE)
            for t in targets: 
                if t in data:
                    del data[t]
            save_json(PROGRESS_FILE, data)
        except Exception as e:
            p_conn.rollback()
            console.print(f"[red]Fehler: {e}[/red]")
    questionary.press_any_key_to_continue().ask()

def action_sqlite_maintenance(config):
    console.print(Panel("[bold]SQLite Wartung[/bold]", box=box.ROUNDED))
    db_path = config["sqlite_path"]
    choice = questionary.select("Aktion wählen:", choices=["VACUUM", "Optimize", "Analyze", "Integrity Check", "Zurück"]).ask()
    if choice == "Zurück":
        return
    s_conn = connect_sqlite_rw(db_path)
    if not s_conn:
        return
    try:
        cur = s_conn.cursor()
        if choice == "VACUUM":
            if questionary.confirm(f"WARNUNG: VACUUM benötigt temporär den doppelten Speicherplatz der DB. Sicher?").ask():
                with console.status("Führe VACUUM durch (Kann lange dauern!)..."):
                    cur.execute("VACUUM;")
                console.print("[green]VACUUM erfolgreich.[/green]")
        elif choice == "Optimize":
            with console.status("Führe WAL Checkpoint durch..."):
                cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                cur.execute("ANALYZE;")
            console.print("[green]Optimierung fertig.[/green]")
        elif choice == "Analyze":
            sel = questionary.checkbox("Tabellen:", choices=[questionary.Choice(t['name']) for t in TABLES_CONF]).ask()
            if sel: 
                with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), TaskProgressColumn()) as progress:
                    task = progress.add_task("Analyze...", total=len(sel))
                    for t in sel: 
                        progress.update(task, description=f"Analyze {t}...")
                        try:
                            cur.execute(f"ANALYZE {t};")
                        except Exception as e:
                            console.print(f"[red]Fehler {t}: {e}[/red]")
                        progress.advance(task)
                console.print("[green]Fertig.[/green]")
        elif choice == "Integrity Check":
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

def action_maintenance(config):
    while True:
        choice = questionary.select("Option:", choices=["Sequenzen Reset", "Repair Center (Smart)", "DB Status", "Postgres VACUUM", "Postgres TRUNCATE", "SQLite Wartung", "Reset Progress", "Zurück"]).ask()
        if choice == "Zurück":
            return
        if choice == "Sequenzen Reset":
            action_sequence_reset(config)
        if choice == "Repair Center (Smart)":
            action_fix_metadata(config)
        if choice == "DB Status":
            action_verify_migration(config)
        if choice == "Postgres VACUUM":
            action_vacuum_menu(config)
        if choice == "Postgres TRUNCATE":
            action_truncate_menu(config)
        if choice == "SQLite Wartung":
            action_sqlite_maintenance(config)
        if choice == "Reset Progress":
            action_reset_progress()

def action_migration_menu(config):
    choice = questionary.select("Modus:", choices=["Alle Tabellen", "Selektiv", "Zurück"]).ask()
    if choice == "Zurück":
        return
    plan = [t['name'] for t in TABLES_CONF] if choice.startswith("Alle") else questionary.checkbox("Wähle Tabellen:", choices=[questionary.Choice(t['name']) for t in TABLES_CONF]).ask()
    if plan:
        run_migration_plan(config, plan)

def main():
    console.clear()
    console.print(Panel.fit("[bold blue]Home Assistant Migration Tool[/bold blue]\n[dim]v69 - Final Production Release[/dim]", box=box.DOUBLE))
    if not os.path.exists(CONFIG_FILE):
        action_create_config()
    config = load_json(CONFIG_FILE)
    if not config:
        return
    
    while True:
        console.print("")
        c = questionary.select("Hauptmenü", choices=["Migration starten", "Wartung & Diagnose", "TimescaleDB Optimierung", "Config", "Beenden"]).ask()
        if c == "Beenden":
            sys.exit(0)
        if c == "Config":
            config = action_create_config()
        if c == "Migration starten":
            action_migration_menu(config)
        if c == "TimescaleDB Optimierung":
            action_convert_schema(config)
        if c == "Wartung & Diagnose":
            action_maintenance(config)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[yellow]Abbruch.[/yellow]")