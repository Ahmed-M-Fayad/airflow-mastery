# airflow-training-ground

A running lab of 12 progressively harder Airflow pipelines, built to learn orchestration
end to end rather than to look polished. Log of what broke and what I learned before the
standalone capstone repo takes over.

**Status: in progress.** This is a learning log, not a portfolio piece — expect rough
edges, commented-out experiments, and commit messages that are more "fixed the retry
loop" than conventional-commit clean. Once all 12 are done, this repo gets archived
(GitHub "Archive this repository") and I stop touching it. The polished, CV-facing
version of the strongest project here lives in a separate repo, linked below once it
exists.

---

## Setup
```
git clone <repo>
cd airflow-training-ground
cp .env.example .env
docker compose up
```
Airflow UI at `localhost:8080`. See `plugins/common/` for shared Hook/Operator code
before assuming a project's logic is self-contained.

## Progress log

| # | Project | Concepts | Status | Notes |
|---|---------|----------|--------|-------|
| 01 | Incremental ETL | idempotency, templating, retries | ☐ | |
| 02 | Weather Ingestion | pagination, rate limits, connections | ☐ | |
| 03 | Data Quality Gate | branching, trigger rules | ☐ | |
| 04 | Multi-Source Fan-In | TaskGroups | ☐ | |
| 05 | Sensor-Driven | FileSensor, reschedule mode | ☐ | |
| 06 | Cross-DAG Orchestration | ExternalTaskSensor / datasets | ☐ | |
| 07 | Custom Hook + Operator | plugin authoring | ☐ | |
| 08 | DAG Testing + CI | pytest, GitHub Actions | ☐ | |
| 09 | Containerized Task | DockerOperator | ☐ | |
| 10 | Observability | callbacks, SLA misses, logging | ☐ | |
| 11 | Capstone — Analytics Mart | dbt orchestration, full pipeline | ☐ | → extract to standalone repo when done |

Mark `☑` and add a one-line note per project as it's finished — the note is for future
me, not for anyone reviewing this. What broke, what the actual "aha" was, not a
restatement of the requirements.

## Connection / Variable registry
Fill in as projects add them — keeps 12 DAGs' worth of connections from becoming a
guessing game.

| ID | Used by | Purpose |
|---|---|---|
| | | |

## Retro (fill in once all 12 are done)
- What Airflow concepts feel solid now vs. still shaky
- What from this repo is actually worth carrying into the capstone repo
- What I'd do differently starting project 1 again
