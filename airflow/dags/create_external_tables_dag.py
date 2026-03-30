"""
Create BigQuery external tables pointing to Parquet files in GCS.
"""

import os
from datetime import datetime

from airflow.sdk import DAG, task
from google.cloud import bigquery

GCS_BUCKET = os.environ["GCS_PROCESSED_BUCKET"]
BQ_DATASET = os.environ.get("BQ_DATASET", "amtab_transit")
ENTITY_TYPES = ["trip_updates", "vehicle_position"]


with DAG(
    dag_id="create_external_tables",
    description="Create BigQuery external tables on GCS Parquet files",
    start_date=datetime(2025, 6, 1),
    schedule=None,
    catchup=False,
    tags=["warehouse"],
):

    @task()
    def create_external_table(entity: str):
        """Create or replace an external table for one entity type."""
        client = bigquery.Client()
        table_id = f"{client.project}.{BQ_DATASET}.{entity}"
        source_uri = f"gs://{GCS_BUCKET}/{entity}/*.parquet"

        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [source_uri]
        external_config.autodetect = True

        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        client.delete_table(table_id, not_found_ok=True)
        table = client.create_table(table)
        print(f"Created external table {table_id}")

    for entity_type in ENTITY_TYPES:
        create_external_table(entity=entity_type)
