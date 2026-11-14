# Kafka notes

## Break-it test

The *break-it test* evaluates the behavior of the producer and consumer after 
the broker is suddenly stopped and restarted. In this case, the producer should 
recover on its own, while the consumer should resume without any gaps, though
duplicates may occur in Parquet due to at-least-once delivery.

Three scenarios were tested:
- Scenario A: Short outage
- Scenario B: Outage longer than `message.timeout.ms`
- Scenario C: Broker crash

Each scenario was tested with `AMTAB_FLUSH_MAX_ROWS=200 make up` and **at least
one flush** to ensure the consumers had committed offsets.

### Scenario A: short outage

In this scenario, the producer loses no messages during a one-minute outage.

Monitor producer and consumer logs via two separate terminals:

```bash
# terminal 1
docker compose --project-directory streaming logs -f producer-vehicles

# terminal 2
docker compose --project-directory streaming logs -f consumer-vehicles
```

Before triggering an outage, the producer sends the messages to the broker:

```log
INFO __main__ [vehicles] 75 entities: 0 delivered, 0 discarded, 0 failed
INFO __main__ [vehicles] 75 entities: 75 delivered, 0 discarded, 0 failed
INFO __main__ [vehicles] 75 entities: 150 delivered, 0 discarded, 0 failed
```

From another terminal, stop Redpanda and restart it after **one minute**:

```bash
docker compose --project-directory streaming down redpanda-0
sleep 60 && docker compose --project-directory streaming up redpanda-0
```

The producer log shows deliveries stay at 225 while messages are queued during 
the outage, and then jump to 463 once they're delivered after the reconnect, 
with zero failures:

```log
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Disconnected: connection closed by peer: receive 0 after POLLIN (after 82041ms in state UP)
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/bootstrap]: redpanda-0:9092/bootstrap: Connect to ipv4#172.22.0.2:9092 failed: Connection refused (after 1ms in state CONNECT)
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Failed to resolve 'redpanda-0:9092': Name or service not known (after 26ms in state CONNECT)
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Failed to resolve 'redpanda-0:9092': Name or service not known (after 37ms in state CONNECT, 1 identical error(s) suppressed)
INFO __main__ [vehicles] 75 entities: 225 delivered, 0 discarded, 0 failed
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/bootstrap]: redpanda-0:9092/bootstrap: Connection setup timed out in state CONNECT (after 30029ms in state CONNECT)
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/bootstrap]: redpanda-0:9092/bootstrap: Failed to resolve 'redpanda-0:9092': Name or service not known (after 23ms in state CONNECT)
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/bootstrap]: redpanda-0:9092/bootstrap: Failed to resolve 'redpanda-0:9092': Name or service not known (after 25ms in state CONNECT, 1 identical error(s) suppressed)
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Failed to resolve 'redpanda-0:9092': Name or service not known (after 34ms in state CONNECT, 10 identical error(s) suppressed)
INFO __main__ [vehicles] 80 entities: 225 delivered, 0 discarded, 0 failed
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/bootstrap]: redpanda-0:9092/bootstrap: Failed to resolve 'redpanda-0:9092': Name or service not known (after 27ms in state CONNECT, 20 identical error(s) suppressed)
FAIL|0fad1dc6148f#producer-1| [thrd:redpanda-0:9092/bootstrap]: redpanda-0:9092/bootstrap: Connect to ipv4#172.22.0.2:9092 failed: Connection refused (after 1ms in state CONNECT)
INFO __main__ [vehicles] 83 entities: 225 delivered, 0 discarded, 0 failed
INFO __main__ [vehicles] 83 entities: 463 delivered, 0 discarded, 0 failed
```

### Scenario B: outage longer than `message.timeout.ms`

Scenario B is tested the same way as Scenario A, but Redpanda is restarted
after 6 minutes. The `librdkafka` timeout value (`message.timeout.ms`) defaults
to 5 minutes: each message still waiting in the producer queue after that
long is dropped and reported as failed.

```bash
docker compose --project-directory streaming down redpanda-0
sleep $((60*6)) && docker compose --project-directory streaming up redpanda-0
```

Once they have waited 5 minutes, the oldest queued messages expire and are 
counted as failed:

```log
FAIL|6ccf98de6a76#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Failed to resolve 'redpanda-0:9092': Name or service not known (after 27ms in state CONNECT, 4 identical error(s) suppressed)
FAIL|6ccf98de6a76#producer-1| [thrd:redpanda-0:9092/bootstrap]: redpanda-0:9092/bootstrap: Failed to resolve 'redpanda-0:9092': Name or service not known (after 25ms in state CONNECT, 20 identical error(s) suppressed)
ERROR stats [-] delivery failed: Local: Message timed out (-192)
... 
INFO __main__ [vehicles] 78 entities: 331 delivered, 0 discarded, 67 failed
```

After 6 minutes of outage, only the messages queued in the first minute had 
expired. That's about one poll, hence 67 failed. Everything queued later was 
delivered once the broker returned.

Alternatively, you can prevent timeout errors by setting `message.timeout.ms=0`.
The trade-off is choosing between data loss after the default timeout expires
and a larger in-memory queue.

### Scenario C: broker crash

In this test, Redpanda is suddenly killed by executing:

```bash
docker compose --project-directory streaming kill -SIGKILL redpanda-0
```

and then restarted:

```bash
docker compose --project-directory streaming up redpanda-0
```

As in Scenario A, the producer successfully delivers messages: 

```log
INFO __main__ [vehicles] 83 entities: 0 delivered, 0 discarded, 0 failed
INFO __main__ [vehicles] 83 entities: 83 delivered, 0 discarded, 0 failed
INFO __main__ [vehicles] 82 entities: 166 delivered, 0 discarded, 0 failed
INFO __main__ [vehicles] 82 entities: 248 delivered, 0 discarded, 0 failed
FAIL|2ccb7fa7f7e3#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Disconnected: connection closed by peer: receive 0 after POLLIN (after 102296ms in state UP)
FAIL|2ccb7fa7f7e3#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Failed to resolve 'redpanda-0:9092': Name or service not known (after 50ms in state CONNECT)
FAIL|2ccb7fa7f7e3#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Failed to resolve 'redpanda-0:9092': Name or service not known (after 26ms in state CONNECT, 1 identical error(s) suppressed)
INFO __main__ [vehicles] 81 entities: 330 delivered, 0 discarded, 0 failed
FAIL|2ccb7fa7f7e3#producer-1| [thrd:redpanda-0:9092/0]: redpanda-0:9092/0: Connect to ipv4#172.22.0.2:9092 failed: Connection refused (after 1ms in state CONNECT)
INFO __main__ [vehicles] 83 entities: 411 delivered, 0 discarded, 0 failed
```

An important point is that killing the process **does not result in lost acks**,
because the host kernel still holds the writes in its page cache. Those would
be lost forever only if the host itself crashes.

### Consumer

The consumer's behavior is verified by querying the bronze layer after all
three scenarios have been tested. Gaps and duplicates are checked by executing
the following query using DuckDB:

```sql
select feed, kafka_partition,
       count(*)                                  as rows,
       count(distinct kafka_offset)              as offsets,
       max(kafka_offset) - min(kafka_offset) + 1 as span
from read_parquet('tmp-bronze/**/*.parquet', hive_partitioning = true)
group by all;
```

Each row carries its partition and offset, so the query checks two things:
- **`rows` > `offsets`** means the consumer wrote some messages twice. 
- **`offsets` < `span`** means a gap: messages that were in the topic never
reached Parquet. That should never happen. If it does, the commit-after-write
logic has a bug.

```
┌──────────┬─────────────────┬───────┬─────────┬───────┐
│   feed   │ kafka_partition │ rows  │ offsets │ span  │
│ varchar  │      int32      │ int64 │  int64  │ int64 │
├──────────┼─────────────────┼───────┼─────────┼───────┤
│ vehicles │               0 │ 20664 │   20597 │ 20597 │
└──────────┴─────────────────┴───────┴─────────┴───────┘
```

The results show **no gaps** are present and **67 rows were written twice** 
(these are not the same 67 rows that failed in Scenario B).
This is consistent with at-least-once delivery: a flush whose offset commit 
failed during an outage, so the consumer re-read and re-wrote those rows after 
the restart.

A leftover temporary file (`_*.parquet`, written before the rename in 
`write_parquet`) could also produce duplicate offsets, but 
`find tmp-bronze -name '_*.parquet'` returns nothing, so all 67 duplicates are 
replays.
