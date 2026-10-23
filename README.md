# airflow-mastery

Hands-on Apache Airflow code from working through *Data Pipelines with Apache Airflow*
(Harenslak & de Ruiter, Manning). It runs on Airflow 3.3.1 with Docker Compose
(CeleryExecutor, Postgres, Redis).

## What's inside

| Path | What it is |
|------|------------|
| `dags/ch02_anatomy_of_a_dag/` | Rocket launches DAG: downloads upcoming launches and their images |
| `dags/ch03_scheduling/` | User stats DAG: fetches events from a local API and computes per-user daily counts |
| `events_api/` | Small mock HTTP API used by chapter 3 (runs as the `events-api` service) |
| `plugins/` | Custom hooks and operators (empty so far) |
| `data/` | Task outputs, bind-mounted to `/opt/airflow/data` (git-ignored) |

Each chapter folder has its own README with what the DAG does and what broke along the way.

## Run it

```bash
git clone https://github.com/Ahmed-M-Fayad/airflow-mastery.git
cd airflow-mastery
cp .env.example .env     # set FERNET_KEY (command inside the file)
mkdir -p logs
docker compose up -d
```

- Airflow UI: `localhost:8080` (login `airflow` / `airflow`)
- Mock events API: `localhost:5000/events`

## Note

The book targets Airflow 2.x, so a few things differ here: operators are imported from
`airflow.providers.standard`, DAGs use `schedule=` instead of `schedule_interval=`, and
`catchup` defaults to `False`.
