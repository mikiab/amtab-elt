import json
from datetime import date

import pytest
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    KafkaException,
    Message,
)
from consumer import (
    Buffer,
    CommitError,
    ConfigurationError,
    FeedConsumer,
    ParquetWriteError,
    PoisonMessage,
    to_row,
    write_parquet,
)
from fake_consumer import FakeConsumer, fatal_error, retriable_error, unknown_error

################################################################################
##                           to_row() tests                                   ##
################################################################################


# Real AMTAB vehicle entity (the "data" part), unchanged from the feed.
VEHICLE_ENTITY = {
    "Id": "3212",
    "IsDeleted": False,
    "TripUpdate": None,
    "Vehicle": {
        "Trip": {
            "TripId": "1516",
            "RouteId": "07",
            "StartDate": "20260825",
            "schedule_relationship": 0,
            "ModifiedTrip": None,
        },
        "Vehicle": {"Id": "3212", "Label": "3212"},
        "Position": {"Latitude": 41.1074867, "Longitude": 16.77307, "Speed": 68.573},
        "CurrentStopSequence": 38,
        "StopId": "10181001",
        "CurrentStatus": 1,
        "Timestamp": 1787637559,
        "congestion_level": 1,
        "MultiCarriageDetails": [],
    },
    "Alert": None,
    "Shape": None,
    "Stop": None,
    "TripModifications": None,
}


VALID_META = {
    "feed": "vehicles",
    "feed_ts": 1787637560,
    "ingest_ts": 1787637562,
    "valid_key": True,
}


# -- Case 1: valid message
VALID = {
    "value": json.dumps({"meta": VALID_META, "data": VEHICLE_ENTITY}).encode("utf-8"),
    "key": b"1516",
    "timestamp": (TIMESTAMP_CREATE_TIME, 1787637559000),
    "partition": 0,
    "offset": 42,
}
# 1787637559000 ms = 2026-08-25 05:59:19 UTC = 07:59:19 Europe/Rome (CEST)
VALID_EXPECTED_DT = date(2026, 8, 25)


def test_to_row_valid():
    msg = Message(**VALID)
    dt, row = to_row(msg)  # pyright: ignore[reportArgumentType]
    assert list(row.keys()) == SCHEMA.names
    assert dt == VALID_EXPECTED_DT
    payload = row.pop("payload")
    assert isinstance(payload, str)
    assert json.loads(payload) == VEHICLE_ENTITY
    assert row == {
        "kafka_partition": 0,
        "kafka_offset": 42,
        "kafka_key": "1516",
        "kafka_event_ts": 1787637559000,
        "feed": "vehicles",
        "feed_ts": 1787637560,
        "ingest_ts": 1787637562,
        "valid_key": True,
    }


# -- Case 2: meta with a missing field (feed_ts)
META_MISSING_FIELD = {k: v for k, v in VALID_META.items() if k != "feed_ts"}

MISSING_META_FIELD = {
    **VALID,
    "value": json.dumps({"meta": META_MISSING_FIELD, "data": VEHICLE_ENTITY}).encode(
        "utf-8"
    ),
}


def test_to_row_missing_field():
    msg = Message(**MISSING_META_FIELD)
    with pytest.raises(PoisonMessage, match="feed_ts"):
        to_row(msg)  # pyright: ignore[reportArgumentType]


# -- Case 3: absurd message timestamp
# Kafka timestamps are int64 milliseconds, so the largest one a real message can
# carry is 2**63 - 1 (confluent_kafka.Message refuses anything bigger).
# That is year 292278994: datetime.fromtimestamp raises ValueError
# ("year ... is out of range"), which date_from_millis must map to its own message.
ABSURD_TIMESTAMP = {
    **VALID,
    "timestamp": (TIMESTAMP_CREATE_TIME, 2**63 - 1),
}


def test_to_row_wrong_timestamp():
    msg = Message(**ABSURD_TIMESTAMP)
    with pytest.raises(PoisonMessage, match="invalid timestamp"):
        to_row(msg)  # pyright: ignore[reportArgumentType]


# -- Case 4
# Use an invalid topic configuration: LogAppendTime instead of CreateTime
USE_LOG_APPEND_TIME = {
    **VALID,
    "timestamp": (TIMESTAMP_LOG_APPEND_TIME, 1787637559000),
}


def test_to_row_raise_configuration_error():
    msg = Message(**USE_LOG_APPEND_TIME)
    with pytest.raises(ConfigurationError):
        to_row(msg)  # pyright: ignore[reportArgumentType]


################################################################################
##                           Buffer tests                                     ##
################################################################################


# -- Case 1: never flush an empty buffer
def test_buffer_empty_flush():
    buf = Buffer()
    assert not buf.should_flush(0)
    assert not buf.should_flush(1000)


# -- Case 2: don't flush before the time interval elapses
def test_buffer_flush():
    row = {"feed": "vehicles"}
    dt = date(2026, 9, 29)
    buf = Buffer(t_interval=300)
    buf.add(dt, row, 0)
    assert not buf.should_flush(299)
    assert buf.should_flush(300)


# -- Case 3: always flush when max size is reached
def test_buffer_max_size_flush():
    row = {"feed": "vehicles"}
    dt = date(2026, 9, 29)
    buf = Buffer(max_batch_size=2)
    for _ in range(2):
        buf.add(dt, row, 0)
    assert buf.should_flush(0)


# -- Case 4: drain() and transition
def test_buffer_drain():
    key = ("vehicles", date(2026, 9, 29))
    row = {"feed": key[0]}
    dt = key[1]
    buf = Buffer(max_batch_size=2, t_interval=300)
    for _ in range(2):
        buf.add(dt, row, 0)
    rows = buf.drain()
    # check the drained content
    assert rows == {key: [row, row]}
    # never flush, neither when the time interval elapses
    assert not buf.should_flush(10000)
    # add a new row at 1000
    buf.add(dt, row, 1000)
    # ensure the timer is restarted
    assert not buf.should_flush(1000 + 300 - 1)
    assert buf.should_flush(1000 + 300)


################################################################################
##                        write_parquet() tests                               ##
################################################################################

import json
from datetime import date

DT = date(2026, 9, 29)

# 1790661600 = 2026-09-29 06:00:00 UTC = 08:00:00 Europe/Rome (CEST)
BASE_TS = 1790661600


def _vehicle_payload(vehicle_id: str, trip_id: str, stop_seq: int, ts: int) -> str:
    """A vehicle entity serialized as the JSON string stored in the payload column."""
    return json.dumps(
        {
            "Id": vehicle_id,
            "IsDeleted": False,
            "TripUpdate": None,
            "Vehicle": {
                "Trip": {
                    "TripId": trip_id,
                    "RouteId": "07",
                    "StartDate": "20260929",
                    "schedule_relationship": 0,
                    "ModifiedTrip": None,
                },
                "Vehicle": {"Id": vehicle_id, "Label": vehicle_id},
                "Position": {
                    "Latitude": 41.1074867,
                    "Longitude": 16.77307,
                    "Speed": 32.5,
                },
                "CurrentStopSequence": stop_seq,
                "StopId": "10181001",
                "CurrentStatus": 1,
                "Timestamp": ts,
                "congestion_level": 1,
                "MultiCarriageDetails": [],
            },
            "Alert": None,
            "Shape": None,
            "Stop": None,
            "TripModifications": None,
        }
    )


ROWS = [
    # Two observations of the same trip, same Kafka partition, consecutive offsets.
    # kafka_event_ts = Vehicle.Timestamp * 1000, as set by the producer.
    {
        "feed": "vehicles",
        "feed_ts": BASE_TS,
        "ingest_ts": BASE_TS + 2,
        "valid_key": True,
        "kafka_partition": 0,
        "kafka_offset": 100,
        "kafka_key": "1516",
        "kafka_event_ts": (BASE_TS - 1) * 1000,
        "payload": _vehicle_payload("3212", "1516", 38, BASE_TS - 1),
    },
    {
        "feed": "vehicles",
        "feed_ts": BASE_TS + 30,
        "ingest_ts": BASE_TS + 32,
        "valid_key": True,
        "kafka_partition": 0,
        "kafka_offset": 101,
        "kafka_key": "1516",
        "kafka_event_ts": (BASE_TS + 29) * 1000,
        "payload": _vehicle_payload("3212", "1516", 39, BASE_TS + 29),
    },
    # Different partition, and a fallback key (valid_key=False): exercises the bool column
    {
        "feed": "vehicles",
        "feed_ts": BASE_TS + 30,
        "ingest_ts": BASE_TS + 33,
        "valid_key": False,
        "kafka_partition": 2,
        "kafka_offset": 57,
        "kafka_key": "4410",
        "kafka_event_ts": (BASE_TS + 27) * 1000,
        "payload": _vehicle_payload("4410", "", 0, BASE_TS + 27),
    },
]


# -- Bad rows: PyArrow REJECTS these with your current SCHEMA
# Each is a one-row batch; from_pylist raises ArrowInvalid / ArrowTypeError,
# which write_parquet must turn into ParquetWriteError, leaving no .parquet file.
_GOOD = ROWS[0]

BAD_ROWS = {
    "offset_not_a_number": [{**_GOOD, "kafka_offset": "x"}],
    "partition_overflows_int32": [{**_GOOD, "kafka_partition": 2**40}],
    "feed_ts_as_string": [{**_GOOD, "feed_ts": "1790661600"}],
    "valid_key_as_int": [{**_GOOD, "valid_key": 1}],
    "payload_not_serialized": [{**_GOOD, "payload": {"Id": "3212"}}],
    # one bad row among good ones: the whole batch must fail, not be half-written
    "bad_row_in_good_batch": [*ROWS, {**_GOOD, "kafka_offset": "x"}],
}


# -- Silent rows: PyArrow ACCEPTS these with your current SCHEMA
# No exception: the data is quietly changed. Not test inputs yet, but a design
# question: which of these should fail, and what would make them fail?
SILENT_ROWS = {
    # missing key -> written as null
    "missing_kafka_key": [{k: v for k, v in _GOOD.items() if k != "kafka_key"}],
    # explicit None -> written as null
    "none_kafka_key": [{**_GOOD, "kafka_key": None}],
    # float seconds -> truncated to 1790661600, the .5 is lost
    "feed_ts_as_float": [{**_GOOD, "feed_ts": BASE_TS + 0.5}],
    # unknown key -> silently dropped (not in SCHEMA)
    "extra_column": [{**_GOOD, "dt": DT}],
}


from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from consumer import SCHEMA


def test_write_parquet(tmp_path: Path):
    file = write_parquet(DT, ROWS, tmp_path)
    assert file.exists()
    # Check if the file is in the right partition
    assert file.parent == tmp_path / f"dt={DT}"
    # Ensure there are no files other than the final Parquet file.
    assert list(file.parent.iterdir()) == [file]
    # Parquet uses "ms" unless the reader passes the schema.
    expected = pa.Table.from_pylist(ROWS, schema=SCHEMA)
    reread = pq.read_table(file, schema=SCHEMA)
    assert expected == reread


@pytest.mark.parametrize("bad_rows", BAD_ROWS.values(), ids=BAD_ROWS.keys())
def test_write_parquet_error(bad_rows: list[dict], tmp_path: Path):
    with pytest.raises(ParquetWriteError):
        write_parquet(DT, bad_rows, tmp_path)

    assert not list(tmp_path.rglob("*.parquet"))


@pytest.mark.parametrize("rows", SILENT_ROWS.values(), ids=SILENT_ROWS.keys())
def test_write_parquet_silent_error(rows: list[dict], tmp_path: Path):
    write_parquet(DT, rows, tmp_path)


################################################################################
##                        FeedConsumer class tests                            ##
################################################################################
""" 
FakeConsumer imitates the parts of confluent_kafka.Consumer that FeedConsumer uses:
  - subscribe(topics, on_revoke=...) stores the callback
  - consume(...) returns scripted batches, then calls `on_exhausted` to stop the loop
  - commit(asynchronous=False) raises the scripted errors in order, then succeeds
  - close() calls on_revoke, like the real client does when it leaves the group


`running` is a module global: stop the loop from `on_exhausted`, e.g.
    FakeConsumer(..., on_exhausted=lambda: monkeypatch.setattr(consumer_module, "running", False))
"""

import consumer
from config import VEHICLES, Settings
from pytest import MonkeyPatch


@pytest.fixture
def create_fake_consumer(monkeypatch: MonkeyPatch):
    def _create_consumer(
        batches: list[list[Message]], commit_errors: list[KafkaException] | None = None
    ):
        fake = FakeConsumer(
            batches,
            commit_errors=commit_errors or [],
            # stop the loop by setting running to False after messages are consumed
            on_exhausted=lambda: monkeypatch.setattr(consumer, "running", False),
        )
        # replace the Kafka Consumer class with the FakeConsumer one
        monkeypatch.setattr(consumer, "Consumer", lambda *args, **kwargs: fake)
        # no-op backoff
        monkeypatch.setattr(consumer.time, "sleep", lambda s: None)
        return fake

    return _create_consumer


# -- Case 1: CommitError after exhausting all retriable error attempts
def test_commit_error(create_fake_consumer, tmp_path: Path):
    fake: FakeConsumer = create_fake_consumer(
        batches=[
            # 1st consume() call returns only one message
            [Message(**VALID)]
        ],
        commit_errors=[retriable_error()] * 6,
    )
    settings = Settings(data_lake_path=tmp_path)
    with pytest.raises(CommitError):
        FeedConsumer(VEHICLES, settings).consume()
    assert fake.commit_attempts == 6


# -- Case 2: two retriable errors, then success returns one commit
def test_commit_success(create_fake_consumer, tmp_path: Path):
    fake: FakeConsumer = create_fake_consumer(
        batches=[
            # 1st consume() call returns only one message
            [Message(**VALID)]
        ],
        commit_errors=[retriable_error()] * 2,
    )
    settings = Settings(data_lake_path=tmp_path)
    FeedConsumer(VEHICLES, settings).consume()
    assert fake.commits == 1
    assert fake.commit_attempts == 3


# -- Case 3: An unknown error results in exactly one attempt, without any retries.
def test_attempts_on_error(create_fake_consumer, tmp_path: Path):
    fake: FakeConsumer = create_fake_consumer(
        batches=[
            # 1st consume() call returns only one message
            [Message(**VALID)]
        ],
        commit_errors=[unknown_error()],
    )
    settings = Settings(data_lake_path=tmp_path)
    with pytest.raises(KafkaException):
        FeedConsumer(VEHICLES, settings).consume()
    assert fake.commit_attempts == 1


# -- Case 4: exactly one commit on normal shutdown
def test_commit_on_shutdown(create_fake_consumer, tmp_path: Path):
    fake: FakeConsumer = create_fake_consumer(
        batches=[
            # 1st consume() call returns only one message
            [Message(**VALID)]
        ],
    )
    settings = Settings(data_lake_path=tmp_path)
    FeedConsumer(VEHICLES, settings).consume()
    assert fake.commits == 1
    assert fake.closed
    assert len(list(tmp_path.rglob("*.parquet"))) == 1


# -- Case 5: failure in the final write results in zero commits
def test_final_write_failure(
    create_fake_consumer, tmp_path: Path, monkeypatch: MonkeyPatch
):
    def fail_write(*args, **kwargs):
        raise ParquetWriteError("disk full")

    monkeypatch.setattr(consumer, "write_parquet", fail_write)

    fake: FakeConsumer = create_fake_consumer(
        batches=[
            # 1st consume() call returns only one message
            [Message(**VALID)]
        ],
    )
    settings = Settings(data_lake_path=tmp_path)
    with pytest.raises(ParquetWriteError):
        FeedConsumer(VEHICLES, settings).consume()
    # a failed write must prevent any commit attempt
    assert fake.commit_attempts == 0
    assert not list(tmp_path.rglob("*.parquet"))
