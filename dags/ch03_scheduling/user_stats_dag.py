import datetime as dt
from pathlib import Path

import pandas as pd
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# `events-api` is the compose service defined in docker-compose.yaml
# (source in ./events_api/app.py). Container hostnames use the service name.
EVENTS_URL = "http://events-api:5000/events"
DATA_DIR = "/opt/airflow/data"

dag = DAG(
    dag_id="user_stats_dag",
    start_date=dt.datetime(2019, 1, 1),
    schedule=None,
    catchup=False,
    tags=["book", "ch03"],
)

fetch_events = BashOperator(
    task_id="fetching_user_events",
    bash_command=(
        f"mkdir -p {DATA_DIR} && "
        f"curl -f -o {DATA_DIR}/events.json {EVENTS_URL}"
    ),
    dag=dag,
)


def _calculate_stats(data_path, output_path):
    events = pd.read_json(data_path)
    stats = events.groupby(["date", "user"]).size().reset_index(name="event_count")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(output_path, index=False)


calc_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "data_path": f"{DATA_DIR}/events.json",
        "output_path": f"{DATA_DIR}/stats.csv",
    },
    dag=dag,
)

fetch_events >> calc_stats
