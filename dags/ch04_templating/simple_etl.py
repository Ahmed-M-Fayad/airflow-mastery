import datetime as dt

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLInsertRowsOperator


@dag(
    dag_id="simple_etl",
    start_date=dt.datetime(2026, 11, 1),
    schedule=None
)

def simple_etl():

    get_data = BashOperator(
        task_id="extracting_data",
        bash_command="""
            cd /opt/airflow/data
            wget -c https://dumps.wikimedia.org/other/pageviews/{{ data_interval_start.strftime('%Y') }}/{{ data_interval_start.strftime('%Y-%m') }}/pageviews-{{ data_interval_start.strftime('%Y%m%d') }}-110000.gz
            gunzip --force /opt/airflow/data/pageviews-{{ data_interval_start.strftime('%Y%m%d') }}-110000.gz
        """,
    )

    @task
    def pageviews_count(pagenames, **context):

        execution_date = context["data_interval_start"]

        filename = (
            f"/opt/airflow/data/"
            f"pageviews-{execution_date.strftime('%Y%m%d')}-110000"
        )

        result = dict.fromkeys(pagenames, 0)

        with open(filename, "r") as f:
            for line in f:
                domain_code, page_title, view_counts, _ = line.split()

                if domain_code == "en" and page_title in pagenames:
                    result[page_title] = int(view_counts)

        # Transform dictionary into rows for PostgreSQL
        rows = [
            (
                pagename,
                pageviewcount,
                execution_date.strftime("%Y-%m-%d %H:%M:%S"),
            )
            for pagename, pageviewcount in result.items()
        ]

        return rows

    count_pages = pageviews_count(
        {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}
    )

    insert_data = SQLInsertRowsOperator(
        task_id="inserting_results_into_postgres",
        conn_id="postgres_conn",
        table_name="pageviews_count",
        columns=["pagename", "pageviewcount", "datetime"],
        rows=count_pages,
    )

    get_data >> count_pages >> insert_data


simple_etl()