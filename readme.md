# Home Assistant Next-Gen Database Migrator (SQLite ➡️ PostgreSQL)

[🇩🇪 Deutsch](#-dokumentation-deutsch) | [🇺🇸 English](#-english-documentation)

---

## 🇩🇪 Dokumentation (Deutsch)

Ein modernes CLI-Tool zur Migration Ihrer Home Assistant Datenbank von SQLite zu PostgreSQL (inklusive TimescaleDB Support). Entwickelt für **große Datenbanken (10GB+)**, Fehlertoleranz und Benutzerfreundlichkeit.

### ✨ Features

* **Modernes UI:** Basiert auf `Rich` & `Questionary`. Steuerung per Pfeiltasten, Checkboxen, farbige Statusbalken.
* **Crash-Safe (Resume):** Speichert den Fortschritt. Bei Abbruch macht das Tool exakt dort weiter, wo es aufgehört hat.
* **Live-Migration:** Home Assistant kann bereits auf die neue DB schreiben, während die Historie importiert wird.
* **Intelligente Selektion:** Wähle Tabellen einzeln (z.B. nur `events`). Das Tool erkennt fehlende Abhängigkeiten automatisch.
* **Wartungs-Suite:** DB-Vergleiche (SQLite vs. Postgres), Sequenz-Korrekturen und VACUUM Tools.
* **TimescaleDB Ready:** Konvertiert Tabellen selektiv in Hypertables (~90% Platzersparnis).

### 📦 Installation

1.  Python 3.8+ benötigt.
2.  Installation:
    ```bash
    pip install -r requirements.txt
    ```

### 🚀 Workflow

Start: `python migrate_ha_modern.py`

1.  **Setup:** Menü **[0] Konfiguration erstellen**.
2.  **Pre-Flight (WICHTIG):** Menü **[3] Wartung** -> **Sequenzen Reset**. (Verhindert ID-Konflikte).
3.  **HA Umstellen:** Home Assistant auf Postgres konfigurieren und neu starten.
4.  **Migration:** Menü **[1] Migration** -> **Alle Tabellen**.
5.  **Optimierung:** Menü **[2] TimescaleDB** (Optional).

---

## 🇺🇸 English Documentation

A modern CLI tool to migrate Home Assistant from SQLite to PostgreSQL.

### ✨ Features

* **Modern UI:** Arrow key navigation, checkboxes, rich progress bars.
* **Resume Capability:** Resumes exactly where left off if interrupted.
* **Live Migration:** Switch HA to the new DB while history imports in the background.
* **Smart Selection:** Select specific tables; dependencies are detected automatically.
* **Maintenance Suite:** DB Comparison tools, Sequence fixing, Vacuum.
* **TimescaleDB:** Convert tables to Hypertables easily.

### 📦 Installation
1.  Python 3.8+ required.
2.  Installation:
    ```bash
    pip install -r requirements.txt
    ```
	
### 🚀 Workflow

Run: `python migrate_ha_modern.py`

1. **Setup:** Menu **[0] Create Config.**
2. **Pre-Flight:** Menu **[3] Maintenance -> Sequence Reset**.
3. **Switch HA:** Configure HA to use Postgres and restart.
4. ** Migrate:** Menu **[1] Migration -> All Tables**.
5. **Optimize:** Menu **[2] TimescaleDB (Optional)**.