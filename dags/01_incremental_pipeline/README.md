# 01 - Incremental ETL

First project in the Airflow mastery track. Generates fake transaction data,
aggregates it, and writes the result to a JSON file — no external API or
database, just enough to actually practice Airflow's scheduling model.

## What it does

- `extract` makes up some fake transactions for the day being processed
- `transform` totals amount + count per category
- `load` writes the summary to `data/loaded/<date>.json`

Data is regenerated the same way every time for a given date (seeded random),
so re-running a day should always give the same result — that was the main
thing I wanted to test with this one: does re-running actually break anything
or not.

## Running it

```bash
docker compose up
```

Then unpause `01_incremental_etl` in the UI and trigger it, or just let it
run on schedule. Output shows up under `data/raw/` and `data/loaded/`.

## What I was practicing

- writing a DAG with the TaskFlow API instead of PythonOperator
- using `{{ ds }}` instead of `datetime.now()` so the pipeline is actually
  tied to the interval it's running for
- making the load step idempotent (overwrite instead of append) so reruns
  don't duplicate data
- retries + retry_delay via default_args

## Todo / later

- point this at the real ETL project instead of fake data, once the basics
  feel solid