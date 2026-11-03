import datetime as dt

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="download_wikipedia_pageviews",
    start_date=dt.datetime.now() - dt.timedelta(days=1),
    schedule=None,
) as dag:

    download_and_extract = BashOperator(
        task_id="download_and_extract",
        bash_command="""
            cd /opt/airflow/data

            wget -c https://dumps.wikimedia.org/other/pageviews/{{ data_interval_start.strftime('%Y') }}/{{ data_interval_start.strftime('%Y-%m') }}/pageviews-{{ data_interval_start.strftime('%Y%m%d') }}-110000.gz
        """,
    )

    download_and_extract