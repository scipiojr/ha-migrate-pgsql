# GitHub Copilot Instructions for HA Migration Tool

You are an expert Python and PostgreSQL developer working on a high-performance migration tool for Home Assistant (SQLite to PostgreSQL).

## 1. Language & Localization (STRICT)
* **User Interface (Console/Logs):** MUST be **German** (Deutsch).
* **Code & Comments:** MUST be **English**.

## 2. Architecture: "The 2-Phase Strategy"
* **PHASE 1 (Metadata):** Migrates dictionary tables while HA is OFF.
* **INTERMISSION:** User starts HA. Script updates sequences (`setval`) to `MAX(id) + BUFFER`.
* **PHASE 2 (Payload):** Migrates heavy history data (`events`, `states`) via backfill while HA is ON.

## 3. Detailed Database Schema (Reference)

You must strictly adhere to this schema definition derived from Home Assistant Core (v2023.4+).

### Level 0: System
* **`schema_changes`**
    * `change_id` (PK, Integer)
    * `schema_version` (Integer)
    * `changed` (Timestamp)

### Level 1: Metadata (Dictionaries)
These tables allow deduplication of strings/JSON to save space.

* **`event_types`**
    * `event_type_id` (PK, Integer)
    * `event_type` (String/Varchar) - *The name of the event (e.g., 'state_changed')*

* **`event_data`**
    * `data_id` (PK, Integer)
    * `hash` (Integer/BigInt) - *Hash for fast lookup*
    * `shared_data` (Text/JSON) - *The actual JSON payload*

* **`states_meta`**
    * `metadata_id` (PK, Integer)
    * `entity_id` (String/Varchar) - *The entity ID (e.g., 'sensor.power')*

* **`state_attributes`**
    * `attributes_id` (PK, Integer)
    * `hash` (Integer/BigInt)
    * `shared_attrs` (Text/JSON) - *The attributes JSON blob*

* **`statistics_meta`**
    * `id` (PK, Integer)
    * `statistic_id` (String/Varchar) - *External ID (e.g., 'sensor.power')*
    * `source` (String) - *Usually 'recorder'*
    * `unit_of_measurement` (String)
    * `has_mean` (Boolean), `has_sum` (Boolean)

### Level 2: Time & Runs
* **`recorder_runs`**
    * `run_id` (PK, Integer)
    * `start` (Timestamp), `end` (Timestamp)
    * `closed_incorrectly` (Boolean), `created` (Timestamp)

* **`statistics_runs`**
    * `run_id` (PK, Integer)
    * `start` (Timestamp)

### Level 3: Payload (The Heavy Data)
These tables reference Level 1 and 2 tables.

* **`events`**
    * `event_id` (PK, Integer)
    * `event_type_id` (FK -> `event_types`)
    * `data_id` (FK -> `event_data`, Nullable)
    * `time_fired` (Timestamp)
    * `time_fired_ts` (Float/Double) - *Unix Timestamp (Critical for Time)*
    * `context_id` (String), `context_user_id` (String), `context_parent_id` (String)

* **`states`**
    * `state_id` (PK, Integer)
    * `metadata_id` (FK -> `states_meta`)
    * `attributes_id` (FK -> `state_attributes`, Nullable)
    * `event_id` (FK -> `events`, Nullable)
    * `last_changed` (Timestamp), `last_changed_ts` (Float)
    * `last_updated` (Timestamp), `last_updated_ts` (Float)
    * `state` (String) - *The actual state value*
    * `old_state_id` (FK -> `states`, Nullable) - *Links to previous state row*

* **`statistics` (Long Term)**
    * `id` (PK, Integer)
    * `metadata_id` (FK -> `statistics_meta`)
    * `created` (Timestamp), `created_ts` (Float)
    * `start` (Timestamp), `start_ts` (Float)
    * `mean` (Float), `min` (Float), `max` (Float)
    * `last_reset` (Timestamp), `last_reset_ts` (Float)
    * `state` (Float), `sum` (Float)

* **`statistics_short_term` (5-Minute Intervals)**
    * *Structure is identical to `statistics`.*

## 4. Technical Standards
* **Batching:** `psycopg2.extras.execute_values`, default batch size **10,000**.
* **Regex:** Use raw strings `r"..."`.
* **JSON:** Use `jsonb` types in Postgres queries (`jsonb_agg`).
* **Connection:** Use `keepalives` to prevent timeouts on large tables.

## 5. Repair Logic (SQL)
To fix "Ghost Data" (Duplicate Entities):
* **Postgres:** `UPDATE statistics SET metadata_id = TARGET WHERE metadata_id = SOURCE`.
* **SQLite:** Requires `REGEXP` injection via Python.