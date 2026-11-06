import datetime as dt

from airflow.sdk import dag
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator

@dag (
    dag_id= "branching",
    start_date= dt.datetime.now() - dt.timedelta(hours=1),
    schedule= None
)

def branching():

    def _pck_print_statement(prnt_stat):
        if prnt_stat == "secret_msg":
            return "secret_msg"
        else:
            return "other"

    pck_statement = BranchPythonOperator(
        task_id = "pck_prnt_statement",
        python_callable= _pck_print_statement,
        op_kwargs= {
            "prnt_stat": "secret_msg"
        }
    )

    def _secret_msg():
        print("SECRET MSG")

    prnt_sec_msg = PythonOperator(
        task_id = "secret_msg",
        python_callable= _secret_msg
    )

    def _other_msg():
        print("MSG")

    prnt_other_msg = PythonOperator(
        task_id = "other",
        python_callable= _other_msg
    )

    pck_statement >> [prnt_sec_msg, prnt_other_msg]

branching()