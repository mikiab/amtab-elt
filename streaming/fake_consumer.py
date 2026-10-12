# feed_consumer.py

"""
Scripted Kafka consumer and ready-made errors.

FakeConsumer imitates the parts of confluent_kafka.Consumer that FeedConsumer uses:
  - subscribe(topics, on_revoke=...) stores the callback
  - consume(...) returns scripted batches, then calls `on_exhausted` to stop the loop
  - commit(asynchronous=False) raises the scripted errors in order, then succeeds
  - close() calls on_revoke, like the real client does when it leaves the group

Inject it by replacing the Consumer name in your consumer module, e.g.:

    fake = FakeConsumer(commit_errors=[retriable_error()])
    monkeypatch.setattr(consumer_module, "Consumer", lambda *args, **kwargs: fake)
    monkeypatch.setattr(consumer_module.time, "sleep", lambda s: None)  # skip backoff

`running` is a module global: stop the loop from `on_exhausted`, e.g.
    FakeConsumer(..., on_exhausted=lambda: monkeypatch.setattr(consumer_module, "running", False))
"""

from collections.abc import Callable

from confluent_kafka import KafkaError, KafkaException, Message


class FakeConsumer:
    def __init__(
        self,
        batches: list[list[Message]] | None = None,
        commit_errors: list[KafkaException] | None = None,
        on_exhausted: Callable[[], None] | None = None,
    ):
        # `batches` is the sequence of consume() results: one inner list per call.
        self._batches = list(batches or [])
        self._commit_errors = list(commit_errors or [])
        self._on_exhausted = on_exhausted
        self._on_revoke = None

        # what the tests assert on
        self.commit_attempts = 0  # every call to commit(), failed or not
        self.commits = 0  # successful commits only
        self.closed = False
        self.revoke_calls = 0

    def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None):
        self.topics = list(topics)
        self._on_revoke = on_revoke

    def consume(self, num_messages=1, timeout=-1):
        if self._batches:
            return self._batches.pop(0)
        if self._on_exhausted is not None:
            self._on_exhausted()
        return []

    def commit(self, message=None, offsets=None, asynchronous=True):
        self.commit_attempts += 1
        if self._commit_errors:
            raise self._commit_errors.pop(0)
        self.commits += 1

    def close(self):
        # The real client revokes the assignment when it leaves the group.
        if self._on_revoke is not None:
            self.revoke_calls += 1
            self._on_revoke(self, [])
        self.closed = True


# Ready-made commit errors


def no_offset_error() -> KafkaException:
    """commit() with nothing to commit: neither fatal nor retriable."""
    return KafkaException(KafkaError(KafkaError._NO_OFFSET, "Local: No offset stored"))


def retriable_error() -> KafkaException:
    """A transient failure, e.g. the coordinator timed out."""
    return KafkaException(
        KafkaError(KafkaError.REQUEST_TIMED_OUT, "request timed out", retriable=True)
    )


def fatal_error() -> KafkaException:
    return KafkaException(KafkaError(KafkaError._FATAL, "fatal error", fatal=True))


def lost_membership_error() -> KafkaException:
    """The consumer is no longer in the group: its partitions belong to someone else."""
    return KafkaException(KafkaError(KafkaError.UNKNOWN_MEMBER_ID, "unknown member id"))


def unknown_error() -> KafkaException:
    """Not fatal, not retriable, not handled explicitly: must not be retried."""
    return KafkaException(
        KafkaError(KafkaError.INVALID_COMMIT_OFFSET_SIZE, "invalid size")
    )
