import datetime as dt

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="wiki_pageviews_analysis",
    start_date=dt.datetime(2026, 10, 28),
    schedule="@hourly",
)
def wiki_pageviews_analysis():

    get_data = BashOperator(
        task_id="get_data",
        bash_command=(
            "curl -o /tmp/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ data_interval_start.strftime('%Y') }}/"
            "{{ data_interval_start.strftime('%Y-%m') }}/"
            "pageviews-{{ data_interval_start.strftime('%Y%m%d-%H') }}0000.gz"
        ),
    )


wiki_pageviews_analysis()