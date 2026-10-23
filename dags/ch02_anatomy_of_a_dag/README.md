# Ch 2 - Anatomy of an Airflow DAG

First DAG from *Data Pipelines with Apache Airflow*: pull the upcoming rocket
launches from the Launch Library 2 API, download each launch's image, and count
what ended up on disk.

## What it does

`download_launches_data >> fetching_launches_images >> notify`

- `download_launches_data` (BashOperator) curls the upcoming-launches JSON
- `fetching_launches_images` (PythonOperator) reads that JSON and downloads every image
- `notify` (BashOperator) prints how many images are in the folder

Output lands in `data/rocket_launches/` (`launches.json` + `images/`).

## Running it

```bash
docker compose up -d
```

Unpause `rocket_launches_dag` in the UI at `localhost:8080` and trigger it
(`schedule=None`, so it only runs when triggered).

## Where I deviated from the book

The book targets Airflow 2.x; this repo runs 3.3.1.

- imports: `airflow.sdk.DAG` and `airflow.providers.standard.operators.*`.
  The old `airflow.operators.*` paths still import but print deprecation warnings
- `start_date`: the book's `days_ago(14)` no longer exists in Airflow 3, and my
  first version used `datetime.now() - 14 days`, which changes every time the file
  is parsed. Now a fixed date (it doesn't matter functionally with `schedule=None`)
- files go to `/opt/airflow/data/rocket_launches/` instead of `/tmp`, so they show
  up on the host in `./data` and I don't need `docker cp` to look at them
- `curl -f` so an HTTP error fails the task instead of saving the error page as
  `launches.json`; `timeout` + `raise_for_status()` on the image downloads for the
  same reason

## Known gaps

- the book uses the `2.0.0` API version; newer versions exist and I haven't checked
  what changed
- the free API tier is rate limited (one source I found says ~15 requests/hour
  without a token), so don't mash the trigger button
- `launch["image"]` can be `None` for some launches. It's caught as
  `MissingSchema` and skipped, which works but is accidental, not designed
