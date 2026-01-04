# Home Assistant DB Migration Tool (SQLite -> PostgreSQL)

Ein professionelles "Next-Gen" Skript zur Migration deiner Home Assistant Historie von der Standard-SQLite-Datenbank auf eine hochperformante PostgreSQL-Instanz.

Es wurde speziell entwickelt, um häufige Probleme wie Foreign-Key-Fehler, Timeouts, "Database Locked"-Fehler und fehlende Historien im Energie-Dashboard ("Geister-Daten") zu vermeiden.

**Version:** 68 (Production Grade)

## ✨ Hauptfunktionen

* **🚀 High-Performance:** Nutzt Batch-Processing (`execute_values`) mit standardmäßig 10.000er Batches und einen speziellen `Fast Mode` (deaktiviert temporär Constraints), um Millionen von Datensätzen in Minuten statt Stunden zu importieren.
* **🟢 Live-Start (Zero Downtime):** Die Migration ist strikt in zwei Phasen unterteilt. Nach Phase 1 (Metadaten) kannst du Home Assistant starten! Die riesige Historie wird im Hintergrund ("Backfill") importiert, während dein Smart Home schon wieder läuft.
* **🔧 Smart Repair Center (Dual-Mode):** Behebt das bekannte Problem fehlender Daten im Energie-Dashboard (wenn HA neue IDs für Sensoren vergibt, z.B. `sensor.power_2`).
    * Funktioniert sowohl auf der **PostgreSQL** (Ziel) als auch auf der **SQLite** (Quelle).
    * Erkennt intelligente Suffixe (auch Mehrkanal-Sensoren wie `_1_2` werden korrekt zu `_1` zugeordnet).
* **🧠 Global Layering Strategy:** Importiert Tabellen strikt nach Abhängigkeiten (Metadaten -> Zeiträume -> Nutzdaten), um Foreign-Key-Fehler zu verhindern.
* **📈 TimescaleDB Support:** (Experimentell) Vorbereitet für TimescaleDB Hypertables, inklusive Sicherheitscheck (verhindert Absturz durch falsche Datentypen).

## 📋 Voraussetzungen

* **Python 3.9+**
* **PostgreSQL Server** (laufend, leer und erreichbar).
* **Home Assistant** (muss für den Start der Migration gestoppt werden).
* Die `home-assistant_v2.db` Datei (am besten lokal kopiert, um "Database Locked"-Fehler zu vermeiden).

## 📦 Installation

1.  Repository klonen oder Dateien herunterladen.
2.  Abhängigkeiten installieren:
    ```bash
    pip install -r requirements.txt
    ```

## 🚀 Nutzung (Der "Live-Start" Workflow)

### 1. Konfiguration
Starten Sie das Tool:
```bash
python migrate_ha_modern_v68.py
```
Ein Wizard führt durch die Einrichtung (Pfade, Passwörter). Dies wird in `config.json` gespeichert.

### 2. Phase 1: Metadaten (HA muss gestoppt sein!)
* Wählen Sie **"Migration starten"** -> **"Alle Tabellen"**.
* Das Skript migriert nun die "kleinen" Tabellen (`states_meta`, `event_types`, etc.). Das geht meist sehr schnell (wenige Sekunden bis Minuten).

### 3. Der Live-Start Moment
Das Skript stoppt automatisch und zeigt eine **große grüne Meldung**.
* Das Tool hat jetzt die Datenbank-Sequenzen (Auto-Increment) in Postgres hochgesetzt (auf Max-ID + 50.000).
* **Aktion:** Konfigurieren Sie jetzt Ihren Home Assistant auf die Postgres-DB und **starten Sie Home Assistant**.
* Home Assistant wird nun *neue* Daten in den "sicheren Bereich" (hohe IDs) schreiben.
* Drücken Sie im Skript eine Taste, um fortzufahren.

### 4. Phase 2: Historie (Hintergrund)
* Das Skript migriert nun die riesigen Tabellen (`events`, `states`, `statistics`).
* Dies geschieht parallel zum laufenden Home Assistant. Da das Skript *alte* IDs (niedrige Nummern) importiert, kommen sie sich nicht in die Quere.

### 5. Nachbereitung & Repair
Nach der Migration fehlen oft Daten im Energie-Dashboard, weil HA beim Starten neue IDs generiert hat.
* Gehen Sie im Skript zu **"Wartung & Diagnose"** -> **"Repair Center (Smart)"**.
* Wählen Sie **"PostgreSQL (Ziel)"**.
* Führen Sie **"Suffixe (_2, _3...) (Smart Merge)"** aus.
* Das Tool analysiert die DB, findet die Duplikate und führt die Historien zusammen.

## ⚠️ Troubleshooting

**Fehler: "server closed the connection unexpectedly"**
Wenn dieser Fehler auftritt, ist das "Paket" von 10.000 Zeilen zu groß für Ihre Verbindung oder den Server-RAM.
* Öffnen Sie die `config.json`.
* Ändern Sie `"batch_size": 10000` auf einen kleineren Wert (z.B. `2000` oder `1000`).
* Starten Sie das Skript neu (es macht genau dort weiter, wo es aufgehört hat).

## Lizenz
MIT License