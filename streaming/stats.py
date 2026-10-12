import logging
from dataclasses import dataclass

from confluent_kafka import KafkaError, Message

logger = logging.getLogger(__name__)


@dataclass
class Stats:
    delivered: int = 0
    failed: int = 0
    discarded: int = 0
    consumed: int = 0

    # called once for each produced message
    # NOTE: Kafka headers are not supported
    def on_delivery(self, err: KafkaError | None, msg: Message):
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            logger.error("delivery failed: %s (%s)", err.str(), err.code())
