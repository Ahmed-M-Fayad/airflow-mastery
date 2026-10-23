import json
import pathlib
from datetime import datetime

import requests
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# The book writes to /tmp and uses `docker cp` to look at the images.
# /opt/airflow/data is bind-mounted to ./data, so outputs show up on the host.
DATA_DIR = pathlib.Path("/opt/airflow/data/rocket_launches")
LAUNCHES_PATH = DATA_DIR / "launches.json"
IMAGES_DIR = DATA_DIR / "images"

dag = DAG(
    dag_id="rocket_launches_dag",
    description="""
        A dag for getting rocket launches data
        then fetching images from it and
        finally saving them to a dir
    """,
    # static date: the book's days_ago(14) is gone in Airflow 3, and a
    # datetime.now()-based start_date changes on every parse
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["book", "ch02"],
)

download_launches = BashOperator(
    task_id="download_launches_data",
    bash_command=(
        f"mkdir -p {DATA_DIR} && "
        f"curl -f -L -o {LAUNCHES_PATH} https://ll.thespacedevs.com/2.0.0/launch/upcoming"
    ),
    dag=dag,
)


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


fetch_images = PythonOperator(
    task_id="fetching_launches_images",
    python_callable=_get_images,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "There are now $(ls {IMAGES_DIR} | wc -l) images."',
    dag=dag,
)

download_launches >> fetch_images >> notify
