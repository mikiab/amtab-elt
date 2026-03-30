"""
Download GTFS-RT archives from GitHub Release, convert JSON to Parquet,
and upload to GCS processed bucket.
"""

import os
import tarfile
import tempfile
import shutil
from datetime import datetime

import duckdb
import requests
from airflow.sdk import DAG, task
from google.cloud import storage

GCS_BUCKET = os.environ["GCS_PROCESSED_BUCKET"]
GITHUB_RELEASE_URL = os.environ["GITHUB_RELEASE_URL"]
ENTITY_TYPES = ["trip_updates", "vehicle_position"]

ENTITY_SQL = {
    "trip_updates": """
        SELECT
            header.Timestamp            AS feed_timestamp,
            e.Id                        AS entity_id,
            e.TripUpdate.Trip.TripId    AS trip_id,
            e.TripUpdate.Trip.RouteId   AS route_id,
            stu.StopSequence            AS stop_sequence,
            stu.StopId                  AS stop_id,
            stu.Arrival.Delay           AS arrival_delay,
            stu.Arrival.Time            AS arrival_time,
            stu.Departure.Delay         AS departure_delay,
            stu.Departure.Time          AS departure_time,
            stu.schedule_relationship   AS schedule_relationship,
            e.TripUpdate.Delay          AS trip_delay,
            dt,
            hour
        FROM read_json('{glob}', hive_partitioning=true) AS raw,
        LATERAL UNNEST(raw.Entities) AS t(e),
        LATERAL UNNEST(e.TripUpdate.StopTimeUpdates) AS t2(stu)
        WHERE e.TripUpdate IS NOT NULL
    """,
    "vehicle_position": """
        SELECT
            header.Timestamp                AS feed_timestamp,
            e.Id                            AS entity_id,
            e.Vehicle.Trip.TripId           AS trip_id,
            e.Vehicle.Trip.RouteId          AS route_id,
            e.Vehicle.Trip.StartDate        AS start_date,
            e.Vehicle.Vehicle.Id            AS vehicle_id,
            e.Vehicle.Vehicle.Label         AS vehicle_label,
            e.Vehicle.Position.Latitude     AS latitude,
            e.Vehicle.Position.Longitude    AS longitude,
            e.Vehicle.Position.Speed        AS speed,
            e.Vehicle.CurrentStopSequence   AS current_stop_sequence,
            e.Vehicle.StopId                AS stop_id,
            e.Vehicle.CurrentStatus         AS current_status,
            e.Vehicle.Timestamp             AS vehicle_timestamp,
            e.Vehicle.congestion_level      AS congestion_level,
            dt,
            hour
        FROM read_json('{glob}', hive_partitioning=true) AS raw,
        LATERAL UNNEST(raw.Entities) AS t(e)
        WHERE e.Vehicle IS NOT NULL
    """,
}


with DAG(
    dag_id="ingest_and_convert",
    description="GitHub Release → JSON → Parquet → GCS",
    start_date=datetime(2025, 10, 1),
    end_date=datetime(2025, 12, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "transformation"],
):

    @task()
    def download_and_extract(entity: str, **context) -> str:
        """Download monthly archive from GitHub Release, extract to temp dir."""
        year_month = context["logical_date"].strftime("%Y-%m")
        filename = f"{entity}_{year_month}.tar.gz"
        url = f"{GITHUB_RELEASE_URL}/{filename}"

        response = requests.get(url, stream=True, timeout=300)
        if response.status_code == 404:
            print(f"No archive at {url}, skipping")
            return ""
        response.raise_for_status()

        extract_dir = tempfile.mkdtemp(prefix=f"{entity}_{year_month}_")
        tmp_archive = os.path.join(extract_dir, filename)

        with open(tmp_archive, "wb") as f:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)

        with tarfile.open(tmp_archive, "r:gz") as tar:
            tar.extractall(path=extract_dir, filter="data")
        os.unlink(tmp_archive)

        file_count = sum(1 for _, _, files in os.walk(extract_dir) for _ in files)
        print(f"Extracted {file_count} files to {extract_dir}")
        return extract_dir

    @task()
    def convert_and_upload(extract_dir: str, entity: str, **context):
        """Convert extracted JSON to Parquet via DuckDB, upload to GCS."""
        if not extract_dir:
            print("No data to convert, skipping")
            return

        year_month = context["logical_date"].strftime("%Y-%m")

        try:
            glob_pattern = os.path.join(extract_dir, "dt=*", "hour=*", "*.json")
            sql = ENTITY_SQL[entity].format(glob=glob_pattern)

            parquet_path = os.path.join(extract_dir, f"{entity}_{year_month}.parquet")

            con = duckdb.connect()
            con.execute(f"COPY ({sql}) TO '{parquet_path}' (FORMAT PARQUET)")
            result = con.execute(f"SELECT count(*) FROM '{parquet_path}'").fetchone()
            row_count = result[0] if result else 0
            con.close()

            print(f"Wrote {row_count} rows to {parquet_path}")

            gcs_path = f"{entity}/{entity}_{year_month}.parquet"
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(parquet_path, timeout=600)

            print(f"Uploaded gs://{GCS_BUCKET}/{gcs_path}")

        finally:
            shutil.rmtree(extract_dir)

    for entity_type in ENTITY_TYPES:
        extracted = download_and_extract(entity=entity_type)
        convert_and_upload(extract_dir=extracted, entity=entity_type)  # type: ignore[arg-type]
