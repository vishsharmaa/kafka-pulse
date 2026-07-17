# KafkaPulse — Consumer Lag & Cluster Health Monitor

A small, focused observability tool for Apache Kafka, written in Java. It polls a
Kafka cluster with the **AdminClient API**, joins every consumer group's committed
offsets against the partitions' log-end offsets, and reports **per-partition consumer
lag** with a traffic-light health status (`OK` / `WARNING` / `CRITICAL`) — the same
core signal tools like Confluent Control Center surface for consumer monitoring.

It ships with a simulated workload (an order-event producer and a consumer-group
worker) so you can create real lag and watch the monitor react.

```
┌───────────┐     orders (3 partitions)     ┌──────────────────┐
│ producer  │ ────────────────────────────▶ │ consumer group   │
│ (produce) │                               │ orders-processor │
└───────────┘                               └──────────────────┘
                       ▲ committed offsets / end offsets
                       │ (AdminClient, every 5s)
                 ┌───────────┐    JSON     ┌──────────────────┐
                 │  monitor  │ ──────────▶ │ :8080/metrics    │
                 └───────────┘             └──────────────────┘
```

## Why lag, and why worst-partition?

Consumer lag is the number of records written but not yet processed. Total lag
hides incidents: a group can look "almost caught up" in aggregate while one stuck
partition quietly falls hours behind (e.g., a poison-pill message or a slow key).
KafkaPulse therefore derives health from the **worst single partition**, not the sum.

## Run it

Requires Java 11+, Maven, and Docker.

```bash
# 1. Start a single-node Kafka (KRaft mode, no ZooKeeper) + create the topic
docker compose up -d

# 2. Build and test
mvn package

# 3. In three terminals:
java -jar target/kafka-pulse-1.0.0.jar produce   # ~50 events/sec
java -jar target/kafka-pulse-1.0.0.jar consume   # keeps up easily
java -jar target/kafka-pulse-1.0.0.jar monitor   # console dashboard + :8080/metrics
```

Watch a lag incident happen:

```bash
# Restart the consumer with 200ms of artificial work per record —
# it can now only handle ~5 events/sec against 50/sec incoming.
PROCESS_DELAY_MS=200 java -jar target/kafka-pulse-1.0.0.jar consume
```

Within a few poll cycles the monitor flips `OK → WARNING → CRITICAL` and
`curl localhost:8080/metrics` shows per-partition lag growing:

```json
{"groups":[{"groupId":"orders-processor","health":"WARNING","totalLag":347,
"maxPartitionLag":128,"timestamp":"...","partitions":[
{"topic":"orders","partition":0,"committed":412,"end":540,"lag":128}, ...]}]}
```

## Configuration

| Env var             | Default          | Meaning                                   |
|---------------------|------------------|-------------------------------------------|
| `BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers                   |
| `EVENTS_PER_SEC`    | `50`             | Producer event rate                       |
| `PROCESS_DELAY_MS`  | `0`              | Artificial per-record processing delay    |
| `WARN_LAG`          | `100`            | Worst-partition lag → `WARNING`           |
| `CRITICAL_LAG`      | `1000`           | Worst-partition lag → `CRITICAL`          |
| `METRICS_PORT`      | `8080`           | Metrics/health HTTP port                  |

## Design notes

- **Lag math is isolated from I/O.** `LagCalculator` and the model classes are pure
  and fully unit-tested (13 tests, including edge cases: never-committed partitions,
  stale end-offset snapshots, threshold boundaries). The AdminClient plumbing in
  `LagMonitor` stays thin.
- **At-least-once consumer.** Manual `commitSync()` *after* processing — a crash
  mid-batch redelivers rather than loses records.
- **Idempotent producer with `acks=all`** — the right durability trade-off for
  order-like events.
- **Zero web dependencies.** The metrics endpoint uses the JDK's built-in
  `HttpServer`; the only runtime dependency is `kafka-clients`.

## Tests

```bash
mvn test
```
