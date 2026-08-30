import requests
import os
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.models.param import Param

DATA_DIR = "/opt/airflow/data"
BOOKS_DIR = os.path.join(DATA_DIR, "books")


@dag(
    dag_id="02_api_incremental_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["mastery-track"],
    params={
        "book_title": Param(
            "Atomic Habits",
            type="string",
            description="the book title to search with",
        )
    },
)
def api_ingestion():

    @task(
        execution_timeout=timedelta(minutes=5),
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
    def book_search(book_title: str) -> dict:
        url = "https://openlibrary.org/search.json"

        all_docs = []
        page = 1
        num_found = None

        while num_found is None or len(all_docs) < num_found:
            response = requests.get(
                url,
                params={"title": book_title, "page": page, "limit": 100},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            if num_found is None:
                num_found = data["numFound"]

            all_docs.extend(data["docs"])

            if not data["docs"]:
                break  

            page += 1
            if page > 10:
                break  

        return {"docs": all_docs}

    @task
    def filter_response(response: dict) -> list[dict]:
        docs = response["docs"]
        books = []

        for doc in docs:
            books.append({
                "Book": doc.get("title"),
                "Author": doc.get("author_name"),
                "Publish Year": doc.get("first_publish_year"),
                "Editions": doc.get("edition_count"),
            })

        return books

    @task
    def save(books: list[dict], ds: str = None) -> None:
        import json

        os.makedirs(BOOKS_DIR, exist_ok=True)
        path = os.path.join(BOOKS_DIR, f"{ds}.json")

        with open(path, "w") as f:
            json.dump(books, f, indent=4)

        print(f"saved {len(books)} books to {path}")

    save(filter_response(book_search("{{ params.book_title }}")))


api_ingestion()