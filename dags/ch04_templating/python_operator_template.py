import datetime as dt
import requests

from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator


@dag(
    dag_id="python_operator_template",
    start_date=dt.datetime(2026, 10, 30),
    schedule=None,
)
def python_operator_template():

    def _get_data(data_interval_start, **context):
        year, month, day, hour, *_ = data_interval_start.timetuple()

        url = (
            "https://dumps.wikimedia.org/other/pageviews/"
            f"{year}/{year}-{month:02d}/"
            f"pageviews-{year}{month:02d}{day:02d}-{hour:02d}0000.gz"
        )

        output_path = "/opt/airflow/data/wikipageviews.gz"

        response = requests.get(url)

        with open(output_path, "wb") as f:
            f.write(response.content)

    get_data = PythonOperator(
        task_id="get_pageviews_data",
        python_callable=_get_data,
    )


python_operator_template()