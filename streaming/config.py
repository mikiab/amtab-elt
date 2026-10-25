# config.py

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import NonNegativeInt, PositiveFloat
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class FeedConfig:
    """Configuration of a single GTFS-RT feed endpoint and its Kafka topic."""

    name: str
    url: str
    topic: str
    dlq_topic: str
    entity_field: str

    def key(self, message: dict[str, Any]) -> tuple[str | None, bool]:
        entity = message["data"]
        trip: dict | None = entity[self.entity_field].get("Trip")
        if isinstance(trip, dict):
            trip_id: str | None = trip.get("TripId")
            if trip_id:
                return trip_id, False
        return entity.get("Id", None), True

    def event_time(self, message: dict[str, Any]) -> int:
        meta, entity = message["meta"], message["data"]
        if meta["feed"] == VEHICLES.name:
            ts: int | None = entity[self.entity_field].get("Timestamp")
            if ts:
                return ts
        return meta["feed_ts"]


CompressionType = Literal["none", "gzip", "snappy", "lz4", "zstd"]


class Settings(BaseSettings):
    kafka_bootstrap_servers: str

    # Producer
    kafka_linger_ms: NonNegativeInt = 100
    kafka_compression_type: CompressionType = "lz4"
    poll_interval_s: PositiveFloat = 30.0

    # HTTP
    http_timeout_s: PositiveFloat = 10.0
    http_max_retries: NonNegativeInt = 3
    http_backoff_base_s: PositiveFloat = 1.0

    # Data Lake path
    data_lake_path: Path

    model_config = SettingsConfigDict(env_file=".env", env_prefix="AMTAB_")

    # Producer settings affecting the retry mechanism
    # - acks
    # - retries
    # - delivery.timeout.ms
    # idempotency enables acks=all, illimitate retries and max.in.flight=5
    # ensuring no duplicates and re-ordering in the case of retries
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "linger.ms": self.kafka_linger_ms,
            "compression.type": self.kafka_compression_type,
            "enable.idempotence": True,
        }

    def consumer_config(self) -> dict:
        """Get the configuration parameters for the consumer"""
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "enable.auto.commit": False,
        }


VEHICLES = FeedConfig(
    name="vehicles",
    url=os.getenv("FEED_VEHICLES_URL", ""),
    topic="vehicle-positions",
    dlq_topic="vehicle-positions.dlq",
    entity_field="Vehicle",
)

TRIPS = FeedConfig(
    name="trips",
    url=os.getenv("FEED_TRIPS_URL", ""),
    topic="trip-updates",
    dlq_topic="trip-updates.dlq",
    entity_field="TripUpdate",
)

FEEDS = {f.name: f for f in (VEHICLES, TRIPS)}

TOPICS = {f.topic: f for f in (VEHICLES, TRIPS)}
