import json
import logging
import signal
import socket
import time

from config import FEEDS, TRIPS, VEHICLES, FeedConfig, Settings
from confluent_kafka import Producer
from feed import FeedUnavailable, fetch_feed
from stats import Stats

logger = logging.getLogger(__name__)

running: bool = True

GTFS_RT_URI = "https://avl.amtab.it/WSExportGTFS_RT/api/gtfs"


def handle_signal(signum, _):
    global running
    running = False
    logger.info("Shutdown requested (signal: %s)", signum)


def new_message(entity: dict, feed: str, feed_ts: int | None) -> dict:
    return {
        "meta": {
            "feed": feed,
            "feed_ts": feed_ts,
            "ingest_ts": int(time.time()),
            "valid_key": True,
        },
        "data": entity,
    }


# Bronze: only check if the response is malformed
def produce(feed_config: FeedConfig):
    settings = Settings()
    stats = Stats()
    producer = Producer(
        {
            **settings.producer_config(),
            "client.id": socket.gethostname(),
        }
    )
    while running:
        count: int = 0
        t0 = time.monotonic()
        producer.poll(0.0)
        try:
            feed = fetch_feed(
                feed_config.url,
                settings.http_timeout_s,
                settings.http_max_retries,
                settings.http_backoff_base_s,
            )
            feed_ts: int = feed["Header"]["Timestamp"]
            for count, entity in enumerate(feed["Entities"], start=1):
                try:
                    message = new_message(entity, feed_config.name, feed_ts)
                    key, is_fallback = feed_config.key(message)
                    if key:
                        message["meta"]["valid_key"] = not is_fallback
                        event_time = feed_config.event_time(message)
                        # Publishing is asynchronous, meaning the delivery
                        # status of the message is handled inside the callback.
                        producer.produce(
                            feed_config.topic,
                            json.dumps(message),
                            str(key).encode("utf-8"),
                            timestamp=event_time * 1000,
                            on_delivery=stats.on_delivery,
                        )
                    else:
                        stats.discarded += 1
                        logger.warning(
                            "message discarded",
                            extra={
                                "feed": feed_config.name,
                                "entity_id": entity.get("Id"),
                                "feed_ts": feed_ts,
                                "reason": "missing key",
                            },
                        )
                except (ValueError, TypeError) as exc:
                    stats.discarded += 1
                    logger.warning(
                        "message discarded",
                        extra={
                            "feed": feed_config.name,
                            "entity_id": entity.get("Id"),
                            "feed_ts": feed_ts,
                            "reason": exc,
                        },
                    )
        except FeedUnavailable as exc:
            logger.warning(
                "feed unavailable",
                extra={
                    "feed": feed_config.name,
                    "reason": exc,
                },
            )
        except Exception as exc:
            logger.fatal(
                "unexpected error",
                extra={
                    "feed": feed_config.name,
                    "reason": exc,
                },
            )
            break

        logger.info(
            "%d entities: %d delivered, %d discarded, %d failed",
            count,
            stats.delivered,
            stats.discarded,
            stats.failed,
            extra={
                "feed": feed_config.name,
            },
        )

        elapsed = time.monotonic() - t0
        time.sleep(max(0, settings.poll_interval_s - elapsed))

    remaining = producer.flush(timeout=10)
    if remaining > 0:
        logger.error(
            "shutdown with %d messages not delivered",
            remaining,
            extra={
                "feed": feed_config.name,
            },
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="A Kafka producer for the AMTAB GTFS-RT feeds"
    )
    parser.add_argument("--feed", type=str, choices=FEEDS, required=True)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    produce(FEEDS[args.feed])
