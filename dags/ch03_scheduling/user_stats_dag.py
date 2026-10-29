import datetime as dt
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.bash import BashOperator

EVENTS_URL = "http://events-api:5000/events"
DATA_DIR = "/opt/airflow/data"


@dag(
    dag_id="user_stats_dag",
    start_date=dt.datetime(2026, 10, 23),
    schedule="@daily",
    catchup=True,
)
def user_stats():

    fetch_events = BashOperator(
        task_id="fetching_user_events",
        bash_command=(
            f"mkdir -p {DATA_DIR}/events && "
            f"curl -f -o {DATA_DIR}/events/{{{{ ds }}}}.json "
            f"'{EVENTS_URL}?start_date={{{{ data_interval_start | ds }}}}"
            f"&end_date={{{{ data_interval_end | ds }}}}'"
        ),
    )

    @task
    def calculate_stats():
        context = get_current_context()
        ds = context["ds"]

        data_path = f"{DATA_DIR}/events/{ds}.json"
        output_path = f"{DATA_DIR}/stats/{ds}_stats.csv"

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        events = pd.read_json(data_path)

        if events.empty:
            stats = pd.DataFrame(
                columns=["date", "user", "event_count"]
            )
        else:
            stats = (
                events.groupby(["date", "user"])
                .size()
                .reset_index(name="event_count")
            )

        stats.to_csv(output_path, index=False)

    fetch_events >> calculate_stats()


user_stats()    