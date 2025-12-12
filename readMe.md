# YouTube Trending Data ETL Pipeline

This project is an automated ETL (Extract, Transform, Load) pipeline that fetches trending video statistics from the YouTube Data API v3, processes the data, validates it, and stores it in a SQLite database for historical tracking.

The pipeline is designed to track video performance over time, including views, likes, comments, and daily rank changes within specific regions (configured for Nigeria by default).

## 🚀 Features

* **Extraction:** Fetches the "Most Popular" videos from YouTube via API, handling pagination and saving raw data to JSON.
* **Transformation:** Cleans raw data, formats timestamps, handles list objects (tags), and standardizes data types using Pandas.
* **Validation:** Performs data quality checks to ensure `video_id` uniqueness, non-negative view counts, and schema integrity before loading.
* **Loading:** Implements an **Upsert** strategy (Insert or Replace) into a local SQLite database to prevent duplicates while allowing daily updates.
* **Analytics:** Automatically runs SQL queries to generate CSV reports on:
    * Top Videos by View Count.
    * Daily Growth (View momentum).
    * Daily Rank Movers (Videos climbing or falling on the trending tab).
    * New Entries (Videos that entered trending today).
    * Channel Insights (Total views per channel).

## 🛠️ Tech Stack

* **Language:** Python 3.x
* **Data Processing:** Pandas, NumPy
* **Database:** SQLite3
* **API:** YouTube Data API v3
* **Orchestration:** Python (via `main.py`)

This repository contains a small ETL pipeline that extracts YouTube "most popular" videos, transforms and validates the data, and stores historical snapshots in a local SQLite database. It also exports a few analytical CSV reports.

This `readMe.md` documents how to set up and run the pipeline, plus troubleshooting notes tailored to the code in `py_scripts/`.

## Quick facts

- Entry point: `main.py`
- ETL modules: `py_scripts/extract.py`, `py_scripts/transform.py`, `py_scripts/validate.py`, `py_scripts/load.py`, `py_scripts/queries.py`
- Raw JSON saved to: `data/` (date-stamped JSON files)
- Reports saved to: `results/`
- Logs: `logs/main.log`

## Project layout

```
.
├── data/                  # raw extracted JSON files (date-stamped)
├── logs/                  # execution logs
├── py_scripts/            # ETL logic
│   ├── extract.py         # fetch from YouTube API
│   ├── transform.py       # build dataframe, normalize fields
│   ├── validate.py        # data quality checks
│   ├── load.py            # inserts/upserts into sqlite
│   └── queries.py         # SQL queries -> CSV reports
├── results/               # generated CSV reports
├── main.py                # pipeline entry point
├── requirements.txt       # dependencies
└── readMe.md              # this file
```


## Requirements

- Python 3.8+ (3.10+ recommended)
- Install dependencies:

```powershell
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the repository root with your YouTube API key:

```
YOUTUBE_API_KEY=your_api_key_here
```

Optional environment variables used by the code (if present):
- `region` (default set to `NG` in `main.py`) — YouTube region code
- `maxResult` (default 50) — number of results per API request

## Run the pipeline

Run from the repository root:

```powershell
python main.py
```

The pipeline will:
- extract trending videos from YouTube (raw JSON saved to `data/`)
- transform the JSON into a Pandas DataFrame
- run validation checks
- load/upsert rows into the SQLite DB (database file created where `load.py` points to)
- export reports to `results/`

Check `logs/main.log` for detailed runtime logs.

## Typical outputs

- `results/top_videos_by_views.csv`
- `results/daily_growth.csv`
- `results/daily_rank_movement.csv`
- `results/new_entries.csv`
- `results/channel_insights.csv`

## Troubleshooting

Below are common issues you might encounter while running the pipeline and how to address them.

1) You get `Data fetched successfully from YouTube API with response code 400` followed by `KeyError: 'items'`

   - Cause: the extractor treated a non-200 response as success and then tried to access `response['items']`.
   - Fix: ensure your API key is valid and has quota. Check `logs/main.log` for the full response body. If the response code is not 200 the extractor should log the body and exit.
   - Quick check: open `py_scripts/extract.py` and confirm it only proceeds when the HTTP status is 200 (or acceptable) and that it logs `response.text` for non-200 responses.

2) `TypeError: unhashable type: 'list'` when calling `df.duplicated()` or similar

   - Cause: one or more DataFrame columns (for example, `thumbnail` or `tags`) contain lists. Pandas `duplicated()` tries to hash all columns and fails on unhashable types.
   - Fix: convert list-like columns to strings before calling `duplicated()` (for example `df['tags'] = df['tags'].astype(str)`), or check duplicates only on selected columns: `df.duplicated(subset=['video_id', 'channel_id'])`.

3) SQLite insertion errors: "type 'method' is not supported" or binding errors

   - Cause: code is passing Series objects, methods, or non-scalar pandas types (Timestamp, list) to `cursor.execute()`.
   - Fix: iterate rows and pass scalars. Convert pandas Timestamps with `.isoformat()` or `str()`, convert lists to JSON strings (`json.dumps()`), and convert numeric NaNs to `None`.
   - Check `py_scripts/load.py` — it should loop `for _, row in df.iterrows():` and insert scalar values.

4) Table `youtube_data` missing when running queries

   - Cause: `load.py` didn't create/commit the table, or the code connects to a different DB filename than the queries use.
   - Fix: verify `sqlite3.connect(...)` uses the same path in both `load.py` and `queries.py`. Make sure `CREATE TABLE IF NOT EXISTS youtube_data (...)` is executed before queries and `conn.commit()` is called after inserts.

## Quick debugging commands (PowerShell)

Capture a full run log:

```powershell
python main.py 2>&1 | Tee-Object -FilePath run.log
code run.log
```

Inspect DB tables:

```powershell
python - <<'PY'
import sqlite3
conn = sqlite3.connect('youtube_data.db')
cur = conn.cursor()
print([r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table';")])
conn.close()
PY
```

Check which files call sqlite3.connect (to find mismatched DB paths):

```powershell
# list occurrences in repo
Select-String -Path .\* -Pattern "sqlite3.connect" -SimpleMatch
```

## Notes & next improvements

- Add retry/backoff for transient API errors.
- Add unit tests for `transform.py` and `validate.py` (small fixtures covering list columns, missing fields, and timestamp conversions).
- Add a script to re-create the DB schema (`scripts/init_db.py`) for easier local setup.