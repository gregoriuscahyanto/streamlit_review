# Streamlit Review App

Diese App ist ein Multi-User-Review-Frontend fuer Fahrzeug-Paare mit Batch-Locking, Vergleichsansicht und Speicherung von Review-Entscheidungen in PostgreSQL.

## Features

- Vergleich von `left_payload` und `right_payload` inkl. Toleranzbewertung fuer numerische Werte
- Farbliche Markierung:
  - Gruen: exakt gleich
  - Gelb: innerhalb Toleranz / normalisiert gleich
  - Rot: unterschiedlich
- Batch-basiertes Claiming von Faellen fuer Reviewer
- Lock-Mechanik pro Fall (`locked_by`, `locked_at`) inkl. Cleanup stale Locks
- Lokale Drafts im Session-State pro Batch
- Aktionen:
  - `Zurueck`
  - `Weiter`
  - `Speichern und Beenden`
- Mobile- und Desktop-Layout
- Logging beim Start ueber `run_streamlit.bat`

## Tech Stack

- Python
- Streamlit
- Pandas
- SQLAlchemy
- PostgreSQL (psycopg2)

## Projektstruktur

- `app.py`: Hauptanwendung
- `run_streamlit.bat`: Startscript mit Zeitstempel-Logfile in `logs/`
- `.streamlit/secrets.toml`: Secrets (DB-Verbindung)
- `.streamlit/config.toml`: UI-Konfiguration (Theme, etc.)
- `logs/`: Laufzeitlogs

## Voraussetzungen

- Python-Umgebung mit Abhaengigkeiten aus `requirements.txt`
- Streamlit CLI im Environment verfuegbar
- Zugriff auf PostgreSQL

Installation (Beispiel):

```bash
pip install -r requirements.txt
```

## Konfiguration

Die App erwartet in Streamlit Secrets:

```toml
DB_URL = "postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME"
```

Datei: `.streamlit/secrets.toml`

## Erwartete Tabellen/Spalten (Schema `review_dev`)

Die App greift auf folgende Tabellen zu:

- `review_dev.review_runs`
- `review_dev.review_cases`
- `review_dev.review_labels`

Wichtige Spalten in `review_cases` (u. a.):

- `run_id`
- `pair_key`
- `status` (`open`, `in_review`, `reviewed`)
- `locked_by`
- `locked_at`
- `updated_at`
- Felder fuer Payload/Score (werden in der App gelesen)

Falls Lock-Spalten fehlen, zeigt die App einen SQL-Hinweis. Zielstruktur:

```sql
ALTER TABLE review_dev.review_cases
ADD COLUMN IF NOT EXISTS locked_by text,
ADD COLUMN IF NOT EXISTS locked_at timestamptz;
```

## Starten der App

### Variante 1 (empfohlen unter Windows)

```bat
run_streamlit.bat
```

Das Script:

- startet `streamlit run app.py`
- legt eine Logdatei an: `logs/run_YYYYMMDD_HHMMSS.log`
- schreibt `stdout` und `stderr` in die Logdatei (inkl. Info/Warning/Error)

### Variante 2 (direkt)

```bash
streamlit run app.py
```

## Bedienablauf

1. Reviewer angeben
2. App claimt einen lokalen Batch aus offenen Faellen
3. Entscheidung pro Fall treffen:
   - `BLOCK_OK`
   - `BLOCK_NOK`
   - `UNSURE`
4. Optional Kommentar erfassen
5. Mit `Weiter` zum naechsten Fall, oder mit `Speichern und Beenden` Batch persistieren und Locks freigeben

Hinweise:

- Ein kontinuierlicher Auto-Refresh ist entfernt; Reruns erfolgen nur aktionsgetrieben.
- Stale Locks werden serverseitig regelmaessig bereinigt (`locked_at::timestamptz`-Vergleich).

## Relevante Konstanten in `app.py`

- `CLAIM_TIMEOUT_MINUTES = 30`
- `DEFAULT_BATCH_SIZE = 10`
- `MAX_BACK_HISTORY = 5`
- `DECISION_OPTIONS = ["BLOCK_OK", "BLOCK_NOK", "UNSURE"]`

## Troubleshooting

- Fehler `Secret DB_URL fehlt.`:
  - `DB_URL` in `.streamlit/secrets.toml` setzen.
- DB-Verbindungsfehler:
  - Netzwerkzugriff, Host/Port, Credentials und Firewall pruefen.
- Keine offenen Faelle:
  - App zeigt an, dass aktuell keine frei verfuegbaren Faelle vorhanden sind.

