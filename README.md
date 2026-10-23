# airflow-mastery

My Airflow learning log: working through *Data Pipelines with Apache Airflow*
(Harenslak & de Ruiter, Manning) chapter by chapter, running on **Airflow 3.3.1**
rather than the 2.x the book was written for.

**Status: in progress.** This is a learning log, not a portfolio piece: expect rough
edges, commented-out experiments, and commit messages that are more "fixed the retry
loop" than conventional-commit clean. Once the book is done, this
repo gets archived (GitHub "Archive this repository") and I stop touching it.

---

## Setup
```bash
git clone https://github.com/Ahmed-M-Fayad/airflow-mastery.git
cd airflow-mastery
cp .env.example .env        # then fill in FERNET_KEY (command is in the file)
mkdir -p logs
docker compose up -d
```
- Airflow UI: `localhost:8080` (login `airflow` / `airflow`, the compose defaults)
- Mock events API (ch 3): `localhost:5000/events`
- Task outputs land in `./data/` (bind-mounted to `/opt/airflow/data`, git-ignored)
- Shared Hook/Operator code, once I write any, goes in `plugins/`

## Layout
```
dags/          one folder per chapter, DAGs tagged ["book", "chNN"]
events_api/    tiny mock HTTP API used by ch 3 (runs as the events-api service)
plugins/       custom hooks/operators (empty so far)
config/        airflow.cfg is generated here and git-ignored
data/          task outputs (git-ignored)
```

## Progress log

☐ not started · ◐ in progress · ☑ done

| Ch | Title | Status | Notes |
|----|-------|--------|-------|
| 01 | Meet Apache Airflow | ☐ | |
| 02 | Anatomy of an Airflow DAG | ◐ | [rocket launches DAG](dags/ch02_anatomy_of_a_dag/) written, not run end to end yet |
| 03 | Scheduling in Airflow | ◐ | [user stats DAG](dags/ch03_scheduling/) works; scheduling / templated dates still to do |
| 04 | Templating tasks using the Airflow context | ☐ | |
| 05 | Defining dependencies between tasks | ☐ | |
| 06 | Triggering workflows | ☐ | |
| 07 | Communicating with external systems | ☐ | |
| 08 | Building custom components | ☐ | |
| 09 | Testing | ☐ | |
| 10 | Running tasks in containers | ☐ | |
| 11 | Best practices | ☐ | |
| 12 | Operating Airflow in production | ☐ | |
| 13 | Securing Airflow | ☐ | |
| 14 | Project: fastest way around NYC | ☐ | |
| 15-18 | Airflow in the clouds (AWS / Azure / GCP) | ☐ | |

Chapter numbers follow the first edition. Check them against your copy if they drift.

Mark `☑` and add a one-line note per chapter as it's finished. The note is for future
me: what broke, what the actual "aha" was, not a restatement of the chapter.

## Book (Airflow 2.x) vs. this repo (Airflow 3.3.1)

Checked against 3.3.1 unless marked otherwise.

| Book | Here | Why it matters |
|------|------|----------------|
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` (same for python) | old path still imports but warns |
| `from airflow import DAG` | `from airflow.sdk import DAG` | Airflow 3's authoring interface |
| `days_ago(n)` for `start_date` | fixed `datetime(...)` | `days_ago` is gone (ImportError) |
| `schedule_interval=` | `schedule=` | the old argument is a TypeError now |
| `catchup` defaults to `True` | defaults to `False` | book's backfill demos need `catchup=True` set explicitly |
| `execution_date` in templates | `logical_date` / `data_interval_start` | removed in Airflow 3. Not verified yet, check at ch 4 |
| 2.x webserver | `airflow-apiserver` service | the UI is served by the API server now |
| `/tmp` + `docker cp` to inspect outputs | `/opt/airflow/data` -> `./data` | files are visible on the host |

Lab vs. real deployment gaps:

- tasks pass files through `./data`, which works because that folder is mounted into
  every Airflow container. On a real multi-worker deployment there's no shared disk
  unless you add one, usually object storage
- `docker-compose.yaml` runs CeleryExecutor with Redis and Postgres on one machine.
  Fine for learning the model, not a production topology
- the events API is a mock. Real APIs have auth, rate limits and schema drift

## Connection / Variable registry
Fill in as DAGs add them. Keeps a pile of DAGs' worth of connections from becoming a
guessing game.

| ID | Used by | Purpose |
|---|---|---|
| | | |

## Retro (fill in once everything is done)
- What Airflow concepts feel solid now vs. still shaky
- What from this repo is worth carrying into real projects
- What I'd do differently starting from chapter 1 again
