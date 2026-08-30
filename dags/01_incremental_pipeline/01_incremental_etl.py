import json
import os
import random
import tempfile
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task

DATA_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
LOADED_DIR = os.path.join(DATA_DIR, "loaded")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _atomic_write_json(path, data):
    # write to a temp file first then rename, so we never leave a half-written
    # file if something crashes mid-write
    dir_name = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


@dag(
    dag_id="01_incremental_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    tags=["mastery-track"],
)
def etl_dag():

    @task
    def extract(ds: str = None) -> list[dict]:
        # fake data generator, seeded by ds so reruns of the same day give
        # the exact same records (needed to actually test idempotency)
        rng = random.Random(ds)
        num_records = rng.randint(15, 30)

        records = []
        for i in range(num_records):
            records.append({
                "id": f"{ds}-{i:03d}",
                "amount": round(rng.uniform(5.0, 500.0), 2),
                "category": rng.choice(["electronics", "groceries", "clothing", "books"]),
                "ds": ds,
            })

        os.makedirs(RAW_DIR, exist_ok=True)
        raw_path = os.path.join(RAW_DIR, f"{ds}.json")
        _atomic_write_json(raw_path, records)

        print(f"extracted {len(records)} records for {ds}")
        return records

    @task
    def transform(records: list[dict]) -> dict:
        # total + count per category
        summary = {}
        for r in records:
            cat = r["category"]
            bucket = summary.setdefault(cat, {"count": 0, "total_amount": 0.0})
            bucket["count"] += 1
            bucket["total_amount"] = round(bucket["total_amount"] + r["amount"], 2)

        return summary

    @task
    def load(summary: dict, ds: str = None):
        os.makedirs(LOADED_DIR, exist_ok=True)
        load_path = os.path.join(LOADED_DIR, f"{ds}.json")

        payload = {
            "ds": ds,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
            "summary": summary,
        }
        _atomic_write_json(load_path, payload)

        print(f"loaded summary for {ds} -> {load_path}")

    load(transform(extract()))


etl_dag()