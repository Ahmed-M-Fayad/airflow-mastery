# Ch 3 - Scheduling in Airflow

Book example: fetch user events from an API and compute per-user, per-day event
counts. The API is a small local stand-in (`events_api/app.py`) that runs as the
`events-api` service in `docker-compose.yaml`, so no external dependency.

## What it does

`fetching_user_events >> calculate_stats`

- `fetching_user_events` curls `http://events-api:5000/events` into `data/events.json`
- `calculate_stats` groups by `(date, user)` with pandas and writes `data/stats.csv`
  (columns: `date, user, event_count`)

The API serves ~3000 random events from the last 30 days (seeded, but dates are
relative to when the container starts). It already accepts `?start_date=` and
`?end_date=` (end exclusive), which the chapter uses later.

## Running it

```bash
docker compose up -d
curl localhost:5000/events | head -c 300     # sanity check the API
```

Unpause `user_stats_dag` and trigger it. `schedule=None` for now.

## What broke (the useful part)

Took four tries to get the first task green:

1. `mkdir -p /data` -> `Permission denied`. The airflow user can't write to `/`
2. `curl localhost:5000` -> connection refused. `localhost` inside the worker is the
   worker itself, not the API container. Container-to-container uses the compose
   service name (`events-api`)
3. right host, but writing to `/data/events.json` still failed (curl exit 23) for
   the same permission reason
4. writing to `/opt/airflow/data` (bind-mounted to `./data`) fixed it. Added `-f` so
   a 4xx/5xx fails the task instead of saving the error body as JSON

## Todo

- [ ] `schedule="@daily"` and a real `start_date`
- [ ] pass `start_date` / `end_date` to the API from the data interval (templating,
      one file per day)
- [ ] try `catchup=True` on purpose: Airflow 3 defaults to `False`, the book assumes
      backfilling
