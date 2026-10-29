import json
import pathlib
from datetime import datetime

import requests
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

DATA_DIR = pathlib.Path("/opt/airflow/data/rocket_launches")
LAUNCHES_PATH = DATA_DIR / "launches.json"
IMAGES_DIR = DATA_DIR / "images"

@dag (
    dag_id="rocket_launches_dag",
    description="""
        A dag for getting rocket launches data
        then fetching images from it and
        finally saving them to a dir
    """,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
)

def rocket_launches():

    download_launches = BashOperator(
        task_id="download_launches_data",
        bash_command=(
            f"mkdir -p {DATA_DIR} && "
            f"curl -f -L -o {LAUNCHES_PATH} https://ll.thespacedevs.com/2.0.0/launch/upcoming"
        )
    )

    @task
    def _get_images():
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)

        with open(LAUNCHES_PATH) as launches_file:
            launches = json.load(launches_file)
            image_urls = [launch["image"] for launch in launches["results"]]

        for image_url in image_urls:
            try:
                response = requests.get(image_url, timeout=30)
                response.raise_for_status()
                image_name = image_url.split("/")[-1]
                image_path = IMAGES_DIR / image_name
                image_path.write_bytes(response.content)
                print(f"Downloaded Image {image_name}")
            except requests.exceptions.MissingSchema:
                print(f"url: {image_url} appears to be invalid")
            except requests.exceptions.ConnectionError:
                print(f"url: {image_url} is not found")
            except requests.exceptions.HTTPError as e:
                print(f"url: {image_url} returned {e.response.status_code}")

    notify = BashOperator(
        task_id="notify",
        bash_command=f'echo "There are now $(ls {IMAGES_DIR} | wc -l) images."'
    )

    download_launches >> _get_images() >> notify

rocket_launches()
