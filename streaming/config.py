import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
    # Producer
    acks: str = os.getenv("KAFKA_ACKS", "all")
    linger_ms: int = int(os.getenv("KAFKA_LINGER_MS", "100"))
    compression_type: str = os.getenv("KAFKA_COMPRESSION", "lz4")

    # Consumer
    group_id: str = os.getenv("KAFKA_GROUP_ID", "")

    poll_interval_s: float = float(os.getenv("POLL_INTERVAL_SECONDS", "30"))

    # HTTP
    http_timeout_s: float = float(os.getenv("HTTP_TIMEOUT_SECONDS", "10"))
    http_max_retries: int = int(os.getenv("HTTP_MAX_RETRIES", "3"))
    http_backoff_base_s: float = float(os.getenv("HTTP_BACKOFF_BASE_SECONDS", "1"))

    # Data Lake path
    data_lake_path: Path = Path(os.getenv("DATA_LAKE_PATH", ""))

    # Producer settings affecting the retry mechanism
    # - acks
    # - retries
    # - delivery.timeout.ms

    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            "linger.ms": self.linger_ms,
            "compression.type": self.compression_type,
        }

    def consumer_config(self) -> dict:
        """Get the configuration parameters for the consumer"""
        return {"bootstrap.servers": self.bootstrap_servers, "group.id": self.group_id}


VEHICLES = FeedConfig(
    name="vehicles",
    url=os.getenv("FEED_VEHICLES_URL", ""),
    topic="vehicle-positions",
    dlq_topic="vehicle-position.dlq",
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
