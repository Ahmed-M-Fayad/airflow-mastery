import datetime as dt
import json

from airflow.sdk import dag, task

@dag(
    dag_id="inspecting_context_args",
    start_date=dt.datetime(2026, 10, 30),
    schedule="@hourly",
)

def inspecting_context_args():

    @task
    def print_context(**kwargs):
        print(json.dumps(kwargs, indent=4, default=str))

    print_context()

inspecting_context_args()