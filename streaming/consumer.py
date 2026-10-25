# consumer.py

import json
import logging
import os
import signal
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from config import TOPICS, FeedConfig, Settings
from confluent_kafka import (
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from pydantic import BaseModel, ConfigDict, ValidationError
from stats import Stats

logger = logging.getLogger(__name__)


running: bool = True


def handle_signal(signum, _):
    global running
    running = False
    logger.info("Shutdown requested (signal: %s)", signum)


class PoisonMessage(Exception):
    """Raised when an unprocessable message is sent to the DLQ."""


class ConfigurationError(Exception):
    """Raised when a topic has a invalid configuration."""


class ParquetWriteError(Exception):
    """Raised when a Parquet file fails to be written."""


class CommitError(Exception):
    """Raised when an offset commit fails."""

    def __init__(self, err: KafkaError, attempts: int):
        super().__init__(f"commit failed after {attempts} attempts: {err.str()}")
        self.err = err
        self.attempts = attempts


class RestartConsumer(Exception):
    """Stop/restart replay to ensure at-least-once delivery."""


class DLQError(Exception):
    """Raised when it's not possible to create o send a message to the DLQ."""


class Meta(BaseModel):
    feed: str
    feed_ts: int
    ingest_ts: int
    valid_key: bool

    model_config = ConfigDict(strict=True)


class Envelope(BaseModel):
    meta: Meta
    data: dict


def local_tz() -> ZoneInfo:
    return ZoneInfo("Europe/Rome")


def date_from_millis(value: int) -> date:
    try:
        return datetime.fromtimestamp(value / 1000, tz=local_tz()).date()
    except ValueError as exc:
        raise ValueError(f"invalid timestamp value ({value}ms)") from exc


def to_row(msg: Message) -> tuple[date, dict]:
    try:
        payload: bytes | None = msg.value()
        if not payload:
            raise ValueError("empty payload")

        key: bytes | None = msg.key()
        if not key:
            raise ValueError("no key found")

        ts_type, ts = msg.timestamp()
        if ts_type == TIMESTAMP_NOT_AVAILABLE:
            raise ConfigurationError("message timestamp is not available")
        elif ts_type == TIMESTAMP_LOG_APPEND_TIME:
            raise ConfigurationError(
                "topic uses LogAppendTime but CreateTime is required"
            )

        envelope = Envelope.model_validate_json(payload)

        return (
            date_from_millis(ts),
            {
                **envelope.meta.model_dump(),
                "kafka_partition": msg.partition(),
                "kafka_offset": msg.offset(),
                "kafka_key": key.decode("utf-8"),
                "kafka_event_ts": ts,
                "payload": json.dumps(envelope.data),
            },
        )
    except ValueError as exc:
        raise PoisonMessage(str(exc)) from exc


class Buffer:
    """Buffer structure for storing records to be consumed"""

    def __init__(self, max_batch_size=5000, t_interval=60 * 60):
        # the key is (feed, date) because we have a single consumer for both feeds
        self._buffer: dict[tuple[str, date], list[dict]] = {}
        self._max_batch_size: int = max_batch_size
        self._t_interval: int = t_interval
        self._t_start: float | None = None
        self._num_rows: int = 0

    def add(self, dt: date, row: dict, now: float):
        if not self._buffer:
            self._t_start = now
        key: tuple[str, date] = (row["feed"], dt)
        self._buffer.setdefault(key, []).append(row)
        self._num_rows += 1

    def drain(self) -> dict[tuple[str, date], list[dict]]:
        buffer, self._buffer = self._buffer, {}
        self._t_start = None
        self._num_rows = 0
        return buffer

    def should_flush(self, now: float) -> bool:
        if self._t_start is None or not self._buffer:
            return False

        elapsed_s: float = max(0.0, now - self._t_start)
        return elapsed_s >= self._t_interval or self._num_rows >= self._max_batch_size


def current_ts() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


# tz-aware timestamps are stored as UTC instants; Parquet has no seconds unit,
# so "s" comes back as "ms" unless the reader passes the schema.
# TODO: make them not nullable
SCHEMA = pa.schema(
    [
        pa.field("feed", pa.string()),
        pa.field("feed_ts", pa.timestamp("s", tz="UTC")),
        pa.field("ingest_ts", pa.timestamp("s", tz="UTC")),
        pa.field("valid_key", pa.bool_()),
        pa.field("kafka_partition", pa.int32()),
        pa.field("kafka_offset", pa.int64()),
        pa.field("kafka_key", pa.string()),
        pa.field("kafka_event_ts", pa.timestamp("ms", tz="UTC")),
        pa.field("payload", pa.string()),
    ]
)


def write_parquet(dt: date, rows: list[dict], base_path: Path) -> Path:
    dest_path = base_path / f"dt={dt}"
    dest_file = f"part-{current_ts()}-{uuid.uuid4().hex[:8]}.parquet"
    try:
        dest_path.mkdir(exist_ok=True, parents=True)
        table = pa.Table.from_pylist(rows, SCHEMA)
        with NamedTemporaryFile(
            "wb", dir=dest_path, prefix="_", suffix=".parquet", delete_on_close=False
        ) as tmp:
            pq.write_table(table, tmp)
            # Ensure *durability* on the local filesystem.
            # local FS only; object stores have no atomic rename, a single PUT
            # is already atomic
            tmp.flush()
            tmp.close()
            # Ensure *atomicity* renaming the temporary file on the same filesystem
            os.replace(tmp.name, dest_path / dest_file)
            return dest_path / dest_file
    except Exception as exc:
        raise ParquetWriteError(
            f"failed to write {dest_file} to {dest_path}: {exc}"
        ) from exc


class FeedConsumer:
    def __init__(self, feed_config: FeedConfig, settings: Settings):
        self.feed_config: FeedConfig = feed_config
        self.settings: Settings = settings
        self.buffer: Buffer = Buffer()
        self.stats: Stats = Stats()
        self.logger = logging.LoggerAdapter(
            logger, extra={"feed": self.feed_config.topic}, merge_extra=True
        )
        self.uncommitted: int = 0
        self.failed: bool = False

    def _flush_and_commit(self, consumer: Consumer):
        for (feed, dt), rows in self.buffer.drain().items():
            write_parquet(dt, rows, self.settings.data_lake_path / feed)

        if self.uncommitted == 0:
            return

        retry: int = 0
        while True:
            try:
                consumer.commit(asynchronous=False)
                self.uncommitted = 0
                break
            except KafkaException as exc:
                err: KafkaError = exc.args[0]
                if err.code() == KafkaError._NO_OFFSET:
                    # trying to commit nothing
                    break
                if err.fatal():
                    raise
                elif err.code() in (
                    KafkaError.ILLEGAL_GENERATION,
                    KafkaError.UNKNOWN_MEMBER_ID,
                ):
                    # no longer own partitions, another consumer will replay them
                    raise
                elif err.retriable():
                    # timeout, coordinator not available, rebalance in progress
                    retry += 1
                    self.logger.warning(
                        "commit offsets failed: %s (attempt: %d)",
                        err.str(),
                        retry,
                    )
                    if retry > 5:
                        raise CommitError(err, retry) from exc
                    time.sleep(5 * retry)
                else:
                    # unknown error
                    raise

    def _handle_rebalance(
        self, consumer: Consumer, revoked_partitions: list[TopicPartition]
    ):
        """called before rebalancing and after messages have been processed by the consumer"""
        # consume() and close() can trigger a rebalance
        if not self.failed:
            self._flush_and_commit(consumer)

    @contextmanager
    def _create_consumer(self):
        consumer = Consumer(
            {
                **self.settings.consumer_config(),
                "auto.offset.reset": "earliest",
                "group.id": f"amtab-parquet-{self.feed_config.name}",
            }
        )
        try:
            consumer.subscribe(
                [self.feed_config.topic], on_revoke=self._handle_rebalance
            )
            yield consumer
        except:
            self.failed = True
            raise
        finally:
            consumer.close()

    def _create_dlq_msg(self, original: Message, error: str) -> dict:
        original_data: None | bytes = original.value()
        try:
            return {
                "dlq_meta": {
                    "failed_at": current_ts,
                    "error": error,
                    "feed": original.topic(),
                    "kafka_ts": original.timestamp()[1],
                    "kafka_key": original.key(),
                    "kafka_partition": original.partition(),
                    "kafka_offset": original.offset(),
                },
                "orginal_message": json.loads(original_data),  # pyright: ignore[reportArgumentType]
            }
        except (ValueError, TypeError) as exc:
            raise DLQError(str(exc)) from exc

    def consume(self):
        with self._create_consumer() as consumer:
            while running:
                try:
                    messages = consumer.consume(num_messages=100, timeout=0.2)
                    for msg in messages:
                        # A single message can be a message or a notification.
                        # librdkafka performs network stuff in background threads.
                        # Here, the errors are notified and put in the same queue
                        # containing the records returned by consume()
                        if error := msg.error():
                            if error.fatal():
                                raise KafkaException(error)
                            else:
                                self.logger.warning(error.str())
                        else:
                            try:
                                dt, row = to_row(msg)
                                self.buffer.add(dt, row, time.monotonic())
                                self.uncommitted += 1
                                self.stats.consumed += 1
                            except PoisonMessage as exc:
                                self.stats.failed += 1
                                self.logger.warning(
                                    "unprocessable message: send to '%s' DLQ",
                                    self.feed_config.dlq_topic,
                                    extra={
                                        "kafka_ts": msg.timestamp()[1],
                                    },
                                )
                                # self._send_to_dlq(msg, str(exc))
                            except ConfigurationError:
                                self.stats.discarded += 1
                                raise

                    if self.buffer.should_flush(time.monotonic()):
                        self._flush_and_commit(consumer)
                    # TODO: otherwise if there is at least one message in the DLQ :
                    #  flush and the buffer

                except KafkaException as exc:
                    err: KafkaError = exc.args[0]
                    if err.fatal():
                        raise
                    else:
                        self.logger.warning(err.str())
                        raise RestartConsumer()
            # flush pending messages in the buffer.
            self._flush_and_commit(consumer)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="A Kafka consumer for AMTAB GTFS-RT topics"
    )
    parser.add_argument("--topic", type=str, choices=TOPICS, required=True)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        settings = Settings()
        FeedConsumer(TOPICS[args.topic], settings).consume()
    except (
        ParquetWriteError,
        CommitError,
        ConfigurationError,
        KafkaException,
        ValidationError,
    ):
        logger.critical("consumer stopped", exc_info=True)
        sys.exit(1)
    except RestartConsumer:
        sys.exit(1)
