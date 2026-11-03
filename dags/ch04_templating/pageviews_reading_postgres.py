import logging          # avoiding pre/post execution annoying message in logs
import datetime as dt

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="pageviews_reading_postgres",
    start_date=dt.datetime(2026, 11, 1),
    schedule=None
)
def reading_postgres():

    query_data = SQLExecuteQueryOperator(
        task_id="query_data",
        conn_id="postgres_conn",
        sql="SELECT * FROM pageviews_count;",
        do_xcom_push=True,
    )

    @task
    def view_data(records):
        lines = []

        lines.append("\nPAGEVIEWS COUNT REPORT")
        lines.append("-" * 55)
        lines.append(f"{'ID':<5} {'PAGE NAME':<20} {'PAGEVIEWS':>12}")
        lines.append("-" * 55)

        for record in records:
            id, name, views, date = record
            lines.append(f"{id:<5} {name:<20} {views:>12,}")

        lines.append("-" * 55)
        lines.append(f"{'TOTAL:':<25} {sum(r[2] for r in records):>12,}")
        lines.append("-" * 55)

        logging.info("\n%s", "\n".join(lines))

    view_data(query_data.output)


reading_postgres()