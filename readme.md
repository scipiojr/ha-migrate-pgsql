# Home Assistant Next-Gen Database Migrator (v10)

[🇩🇪 Deutsch](#-dokumentation-deutsch) | [🇺🇸 English](#-english-documentation)

---

## 🇩🇪 Dokumentation (Deutsch)

Ein hochmodernes, fehlertolerantes Tool zur Migration Ihrer Home Assistant Datenbank von SQLite zu PostgreSQL (inkl. TimescaleDB).
Entwickelt für **große Datenbanken (10GB+)**, maximale Datensicherheit und Benutzerfreundlichkeit.

### ✨ Features der v10 Edition

* **Modernes UI:** Interaktive Menüs (Pfeiltasten), Checkboxen, farbige Fortschrittsbalken (basierend auf `Rich` & `Questionary`).
* **Intelligente Typ-Konvertierung:** Erkennt automatisch Datentypen in Postgres und konvertiert SQLite-Daten passend (z.B. Datumsstrings in Timestamps, `0/1` in Booleans, Bereinigung von Null-Bytes).
* **Crash-Safe (Resume):** Speichert den Fortschritt. Bei Abbruch macht das Tool exakt dort weiter, wo es aufgehört hat.
* **Granularer Reset:** Sie können den Fortschritt für **einzelne Tabellen** zurücksetzen, falls Sie diese neu migrieren wollen.
* **Live-Migration:** Home Assistant kann bereits die neue DB nutzen, während das Tool im Hintergrund die Historie importiert.
* **Diagnose & Logging:** Detaillierte Fehlerprotokolle in `migration_errors.log` (inkl. SQL-Query und fehlerhaften Daten).
* **TimescaleDB Ready:** Konvertiert Tabellen selektiv in Hypertables (~90% Platzersparnis).

### 📦 Installation

1.  **Voraussetzung:** Python 3.8 oder höher.
2.  Installieren Sie die Abhängigkeiten:

```bash
pip install -r requirements.txt
```

### 🚀 Der Migrations-Workflow

Starten Sie das Tool:
```bash
python migrate_ha_modern_v10.py
```

#### Schritt 1: Setup
Wählen Sie **[Konfiguration bearbeiten]**. Der Assistent führt Sie durch die Einrichtung und prüft die Verbindung.

#### Schritt 2: Pre-Flight (WICHTIG!)
*Bevor* Sie Home Assistant mit der neuen Postgres-Datenbank verbinden:
1.  Gehen Sie zu **Wartung & Diagnose** -> **Sequenzen Reset**.
2.  Das Tool setzt die IDs in Postgres hoch (auf `Max_SQLite_ID + 50000`), damit Home Assistant keine IDs vergibt, die später importiert werden sollen.

#### Schritt 3: Home Assistant umstellen
Stoppen Sie HA, ändern Sie die `configuration.yaml` auf PostgreSQL und starten Sie HA neu.
*HA schreibt nun neue Daten. Das Tool füllt die Lücke der Vergangenheit.*

#### Schritt 4: Migration
1.  Wählen Sie **Migration starten**.
2.  Empfehlung: **Alle Tabellen migrieren**.
3.  Das Tool arbeitet die Daten ab.
    * *Konflikte:* Wenn Daten in Postgres schon existieren (wegen Schritt 3), werden diese übersprungen und im Status als "Skip" angezeigt.
    * *Fehler:* Werden in `migration_errors.log` gespeichert.

#### Schritt 5: Abschluss & Wartung
* Prüfen Sie den Erfolg unter **Wartung & Diagnose** -> **DB Status Check**.
* (Optional) Nutzen Sie **TimescaleDB Optimierung** für die Tabellen `statistics`, `statistics_short_term` etc.
* Führen Sie am Ende **Postgres VACUUM** aus (im Wartungsmenü), um die DB-Performance zu optimieren.

---

## 🇺🇸 English Documentation

A state-of-the-art, fault-tolerant tool to migrate your Home Assistant database from SQLite to PostgreSQL (incl. TimescaleDB).
Designed for **large databases (10GB+)**, maximum data safety, and ease of use.

### ✨ v10 Features

* **Modern UI:** Interactive menus (arrow keys), checkboxes, rich progress bars.
* **Smart Type Conversion:** Automatically detects Postgres target types and converts SQLite data accordingly (e.g., date strings to timestamps, `0/1` to booleans, null-byte cleanup).
* **Crash-Safe (Resume):** Saves progress. If interrupted, the tool resumes exactly where it left off.
* **Granular Reset:** Reset progress for **individual tables** if you need to re-migrate specific parts.
* **Live Migration:** HA can use the new DB while history is imported in the background.
* **Diagnostics & Logging:** Detailed error logs in `migration_errors.log` (includes SQL query and data sample).
* **TimescaleDB Ready:** Selectively converts tables to Hypertables (~90% disk space saving).

### 📦 Installation

1.  **Prerequisite:** Python 3.8+.
2.  Install dependencies:

```bash
pip install -r requirements.txt
```

### 🚀 Migration Workflow

Run the tool:
```bash
python migrate_ha_modern_v10.py
```

#### Step 1: Setup
Select **[Konfiguration bearbeiten]** (Edit Config). The wizard guides you through the setup.

#### Step 2: Pre-Flight (CRITICAL!)
*Before* connecting Home Assistant to the new Postgres database:
1.  Go to **Wartung & Diagnose** (Maintenance) -> **Sequenzen Reset** (Sequence Reset).
2.  The tool updates Postgres IDs to `Max_SQLite_ID + 50000` to prevent ID collisions with new data.

#### Step 3: Switch Home Assistant
Stop HA, update `configuration.yaml` to use PostgreSQL, and restart HA.
*HA is now writing new data. The tool will fill the historical gap.*

#### Step 4: Migration
1.  Select **Migration starten**.
2.  Recommended: **Alle Tabellen migrieren** (All Tables).
3.  Sit back.
    * *Conflicts:* Existing data (from Step 3) is skipped safely and shown as "Skip".
    * *Errors:* Logged to `migration_errors.log`.

#### Step 5: Finalize
* Verify results in **Wartung & Diagnose** -> **DB Status Check**.
* (Optional) Use **TimescaleDB Optimierung**.
* Run **Postgres VACUUM** (in Maintenance menu) to optimize performance.