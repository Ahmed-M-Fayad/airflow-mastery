import datetime as dt

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

FILE_PATH = "/opt/airflow/data/test.txt"

@dag (
    dag_id= "independent_tasks",
    start_date= dt.datetime.now() - dt.timedelta(days=1),
    schedule= None
)

def basic_dependency():

    create_file = BashOperator(
        task_id= "creating_file",
        bash_command= f"touch {FILE_PATH}"
    )

    write_msg_1 = BashOperator(
        task_id= "writing_msg_1",
        bash_command= f"echo 'Hello from task 1' >> {FILE_PATH}"
    )

    write_msg_2 = BashOperator(
        task_id= "writing_msg_2",
        bash_command= f"echo 'Hello from task 2' >> {FILE_PATH}"
    )

    view_file = BashOperator(
        task_id= "viewing_file",
        bash_command= f"cat {FILE_PATH}"
    )

    create_file >> [write_msg_1, write_msg_2] >> view_file

basic_dependency()