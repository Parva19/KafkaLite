# KafkaLite — Lightweight Pub/Sub Message Queue

A Kafka-inspired distributed message queue built from scratch in Python, designed to demonstrate core distributed systems and streaming concepts without external dependencies.

---

## Why I Built This

Apache Kafka is widely used in production systems but its internals are often treated as a black box. This project implements Kafka's core concepts from scratch — partitioned logs, consumer groups, offset tracking, and dead letter queues — to deeply understand how distributed message queues work under the hood.

---

## Architecture
                ┌─────────────────────────────────────────┐
                │                 BROKER                  │
                │                                         │
Producers         │  ┌──────────┐      ┌─────────────────┐  │        Consumers
│  │  Topic   │      │  OffsetManager  │  │
Producer-1 ──────►│  │          │      │                 │  │──────► Consumer-1 (group: billing)
Producer-2 ──────►│  │ P0 │ P1 │ P2   │ group+topic+    │  │──────► Consumer-2 (group: billing)
Producer-3 ──────►│  │    │    │      │ partition→offset│  │──────► Consumer-3 (group: fulfillment)
│  └──────────┘      └─────────────────┘  │
│                                          │
│  ┌──────────────┐  ┌─────────────────┐  │
│  │ ConsumerGroup│  │       DLQ       │  │
│  │  rebalance   │  │  failed messages│  │
│  └──────────────┘  └─────────────────┘  │
└─────────────────────────────────────────┘

---

## Key Concepts Implemented

### Topic Partitioning
Messages are routed to partitions using MD5 key hashing:
- Same key always lands on same partition (ordering guarantee)
- Different keys distributed across partitions (parallelism)
- No key — round robin assignment

### Consumer Groups
- Multiple consumers coordinate within a group to split partitions
- Each partition owned by exactly one consumer per group
- Two groups read same topic independently (independent offsets)
- Automatic rebalancing when consumers join or leave

### Offset Tracking
- Messages are never deleted after consumption
- Each consumer group tracks its own offset per partition
- Independent groups can replay from any offset
- Offset stored as: `group_id + topic + partition → offset`

### Dead Letter Queue (DLQ)
- If partition is full after 3 retries, message goes to DLQ
- DLQ grouped by topic and partition
- Failed messages can be reprocessed later

### Thread Safety
- Per-partition locking — only one writer at a time per partition
- Different partitions lock independently — maximum parallelism
- Lock contention solved by increasing partition count, not thread count

### Async Producers and Consumers
- All producers and consumers run as async coroutines
- Single event loop handles all concurrent producers/consumers
- Memory efficient — no thread per producer overhead

---

## Project Structure
KafkaLite/
├── main.py                        # Entry point
├── Broker/
│   ├── broker.py                  # Core broker — routing, publish, consume
│   ├── Topic/
│   │   └── topic.py               # Topic — holds partitions
│   ├── Partition/
│   │   └── partition.py           # Partition — append only log, thread safe
│   ├── ConsumerGroup/
│   │   └── consumer_group.py      # Group management, partition assignment, rebalance
│   ├── OffsetManager/
│   │   └── offset_manager.py      # Per group per partition offset tracking
│   └── DeadLetterQueue/
│       └── dead_letter_queue.py   # Failed message store
├── Producer/
│   ├── producer.py                # Async producer, message generation
│   └── config.py                  # Producer configuration
├── Consumer/
│   ├── consumer.py                # Async consumer, message processing
│   └── config.py                  # Consumer configuration
└── Logger/
└── logger.py                  # File + console logging

---

## Features

| Feature | Description |
|---|---|
| Topic Partitioning | MD5 key hashing routes messages to partitions |
| Consumer Groups | Partitions split across consumers in a group |
| Independent Offsets | Multiple groups read same topic independently |
| Dead Letter Queue | Failed messages stored for reprocessing |
| Async Producers | Coroutine based, no thread per producer |
| Async Consumers | Coroutine based, poll interval configurable |
| Thread Safe Partitions | Per partition lock for concurrent writes |
| Structured Logging | File + console logging with log levels |
| Partition Rebalancing | Auto rebalance on consumer join/leave |

---

## Getting Started

### Requirements
```bash
Python 3.10+
No external dependencies — standard library only
```

### Run

```bash
git clone https://github.com/yourusername/kafkalite
cd kafkalite
python main.py
```

### Sample Log Output
2026-05-16 11:43:26 | INFO  | producer  | Producer 1 generating message 0
2026-05-16 11:43:26 | INFO  | broker    | Routing producer=1 to topic=orders partition=1
2026-05-16 11:43:26 | INFO  | partition | Producer 1 appending to partition 1
2026-05-16 11:43:26 | INFO  | consumer  | Consumer 1 reading from partition 1 offset=0
2026-05-16 11:43:26 | INFO  | consumer  | Consumer 1 received: producer 1 generate the message id-0

---

## Design Decisions and Tradeoffs

### Per-Partition Locking vs Broker-Level Locking
Chose per-partition locking so producers writing to different partitions never block each other. Broker-level locking would serialize all writes — killing parallelism. Tradeoff: more lock objects, but maximum throughput.

### Async vs Threading
Chose async coroutines over threads for producers and consumers. Threads cost 8MB each — 1000 producers would need 8GB RAM. Async handles thousands of coroutines on a single thread with negligible memory overhead. Tradeoff: async requires careful handling of blocking calls.

### Offset in OffsetManager vs Consumer
Moved offset out of ConsumerConfig into a dedicated OffsetManager keyed by `group_id + topic + partition`. This enables multiple consumer groups to read the same partition independently — impossible if offset lives in the consumer object.

### In-Memory Storage
Messages stored in memory lists for simplicity and speed. Tradeoff: messages lost on restart. File-based persistence is on the roadmap.

---

## Scaling Strategy

| Problem | Solution |
|---|---|
| Lock contention | Increase partition count |
| Too many producers | Async coroutines, no thread overhead |
| Multiple services reading same data | Consumer groups with independent offsets |
| Consumer failure | Idle consumers act as hot standbys |
| Message processing failure | Dead Letter Queue with retry |

---

## Key Takeaways

Building this from scratch made clear why Kafka makes the design choices it does:

- **Partitions are the unit of parallelism** — more partitions = more throughput, not more brokers
- **Offsets beat deletion** — never deleting enables replay, multiple consumers, and crash recovery
- **Consumer groups decouple services** — one topic, many independent consumers, zero coordination needed
- **Lock granularity matters** — partition-level locks outperform broker-level locks significantly

---

## Author

Parva Singhal  
[LinkedIn](https://www.linkedin.com/in/parva-singhal-069179140/) | [GitHub](https://github.com/Parva19)