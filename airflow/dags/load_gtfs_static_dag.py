"""
Download GTFS static schedule from GitHub Release and load CSVs into BigQuery.
"""

import os
import io
import zipfile
import tempfile
from datetime import datetime

import requests
from airflow.sdk import DAG, task
from google.cloud import bigquery

BQ_DATASET = os.environ.get("BQ_DATASET", "amtab_transit")
GITHUB_RELEASE_URL = os.environ["GITHUB_RELEASE_URL"]

TABLES_TO_LOAD = [
    "routes",
    "trips",
    "stops",
    "stop_times",
    "calendar",
    "calendar_dates",
    "shapes",
    "feed_info",
]


with DAG(
    dag_id="load_gtfs_static",
    description="Download GTFS static CSVs from GitHub Release and load into BigQuery",
    start_date=datetime(2025, 6, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "warehouse"],
    params={"year": 2025}, # type: ignore[arg-type]
):

    @task()
    def download_and_extract(**context) -> str:
        """Download GTFS static zip from GitHub Release, extract to temp dir."""
        year = context["params"]["year"]
        url = f"{GITHUB_RELEASE_URL}/gtfs_static_{year}.zip"
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        extract_dir = tempfile.mkdtemp(prefix="gtfs_static_")
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            zf.extractall(extract_dir)

        print(f"Extracted GTFS static files to {extract_dir}")
        return extract_dir

    @task()
    def load_csv_to_bigquery(extract_dir: str, table_name: str):
        """Load a single GTFS CSV into a BigQuery table."""
        csv_path = os.path.join(extract_dir, f"{table_name}.txt")

        if not os.path.exists(csv_path):
            print(f"{table_name}.txt not found, skipping")
            return

        client = bigquery.Client()
        table_id = f"{client.project}.{BQ_DATASET}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        # GTFS allows times >= 24:00:00 for trips past midnight.
        # Force time columns to STRING so BigQuery doesn't reject them.
        if table_name == "stop_times":
            job_config.autodetect = False
            job_config.schema = [
                bigquery.SchemaField("trip_id", "STRING"),
                bigquery.SchemaField("arrival_time", "STRING"),
                bigquery.SchemaField("departure_time", "STRING"),
                bigquery.SchemaField("stop_id", "STRING"),
                bigquery.SchemaField("stop_sequence", "INTEGER"),
                bigquery.SchemaField("stop_headsign", "STRING"),
                bigquery.SchemaField("pickup_type", "INTEGER"),
                bigquery.SchemaField("drop_off_type", "INTEGER"),
                bigquery.SchemaField("shape_dist_traveled", "FLOAT"),
                bigquery.SchemaField("timepoint", "STRING"),
            ]

        with open(csv_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows into {table_id}")

    extracted = download_and_extract()

    for table_name in TABLES_TO_LOAD:
        load_csv_to_bigquery(extract_dir=extracted, table_name=table_name)  # type: ignore[arg-type]
