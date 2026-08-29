# Distributed Message Queue

A distributed message broker built from scratch using **Java and Spring Boot** to understand the core ideas behind systems such as Apache Kafka.

The project focuses on the internals of a distributed event broker: **topics, partitions, persistent logs, consumer groups, replication, acknowledgement semantics, failure detection, and leader failover**.

> This is a learning-oriented implementation intended to explore distributed messaging concepts rather than a production-ready replacement for Kafka.

## Architecture

```text
                         ┌──────────────┐
                         │   Producer   │
                         └──────┬───────┘
                                │
                                ▼
                      ┌───────────────────┐
                      │    Any Broker     │
                      └─────────┬─────────┘
                                │
                         Partition Routing
                                │
                                ▼
                     ┌─────────────────────┐
                     │  Partition Leader   │
                     └──────────┬──────────┘
                                │
                         Append to Log
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
              Leader Replica          Follower Replica
                    │                       │
                    └──── Replication ──────┘


Consumers
    │
    ▼
Consumer Group
    │
    ├── Consumer A → Partition 0, 2
    └── Consumer B → Partition 1
```

The same Spring Boot application runs as multiple independent broker instances. Each broker has its own identity and local persistent storage.

The default Docker setup runs a **3-broker cluster**.

---

## Features

### Topics & Partitions

Messages are organized into topics, and each topic can contain multiple partitions.

Messages with a key are deterministically routed to a partition, which preserves ordering for messages sharing that key.

Each partition maintains its own ordered sequence of offsets.

### Persistent Append-Only Logs

Messages are stored in partition-specific append-only log files.

```text
data/
└── orders/
    ├── partition-0.log
    ├── partition-1.log
    └── partition-2.log
```

Each message contains information such as:

```text
key
value
topic
partition
offset
timestamp
```

Messages remain available after consumption, allowing consumers to read them using offsets.

### Multi-Broker Cluster

The system can run multiple broker instances simultaneously.

The default setup contains:

```text
Broker 1 → localhost:8081
Broker 2 → localhost:8082
Broker 3 → localhost:8083
```

Partitions have a designated leader and replica brokers.

For example:

```text
Partition 0 → Leader B1 → Replica B2
Partition 1 → Leader B2 → Replica B3
Partition 2 → Leader B3 → Replica B1
```

A producer may send a message to **any broker**. The receiving broker determines the correct partition and forwards the request to its leader when necessary.

### Leader/Follower Replication

Partition leaders replicate messages to follower brokers.

Followers store the message using the **same partition offset** assigned by the leader.

This allows a replica to contain a copy of the partition log and become the new leader if necessary.

### Acknowledgement Modes

Producers can choose different durability/latency trade-offs.

| Mode | Behaviour |
|---|---|
| `acks=0` | Returns without waiting for the write |
| `acks=1` | Returns after the partition leader writes the message |
| `acks=all` | Returns after all configured replicas are in sync |

The broker tracks replica offsets and maintains a simplified **In-Sync Replica (ISR)** view for each partition.

### Consumer Groups

Consumers can join named consumer groups.

Partitions are distributed between consumers so that a partition is owned by at most one consumer within the same group.

```text
Topic: orders
Partitions: P0 P1 P2

Consumer Group: order-processors

Consumer A → P0, P2
Consumer B → P1
```

When consumers join, leave, or stop sending heartbeats, partition assignments are recalculated.

Consumers maintain committed offsets so processing can continue from the last acknowledged position.

The consumer API also supports long polling for retrieving new messages.

### Broker Heartbeats & Leader Failover

Brokers periodically exchange heartbeats to detect failures.

If a partition leader becomes unavailable, an alive replica can be promoted to leader.

For example:

```text
Before failure

P0
Leader: B1
Replica: B2

B1 fails
   ↓
Heartbeat timeout
   ↓
B1 marked unavailable
   ↓
B2 promoted

After failure

P0
Leader: B2
Replica: B1
```

Producers can continue sending messages through another broker, which routes them to the newly selected leader.

---

## Running the Project

### Requirements

- Java 21
- Docker
- Docker Compose

The project uses **Spring Boot** and Maven.

### Start the Cluster

Build the application:

```bash
./mvnw clean package
```

Start all three brokers:

```bash
docker compose up --build -d
```

Check the containers:

```bash
docker compose ps
```

### Create a Topic

For the current implementation, create the topic on each broker:

```bash
curl -X POST http://localhost:8081/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"orders","partitions":3}'

curl -X POST http://localhost:8082/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"orders","partitions":3}'

curl -X POST http://localhost:8083/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"orders","partitions":3}'
```

### Produce a Message

A producer can send the request to any broker:

```bash
curl -X POST \
  "http://localhost:8083/topics/orders/messages?acks=all" \
  -H "Content-Type: application/json" \
  -d '{
    "key":"user-123",
    "value":"order-created"
  }'
```

Example response:

```json
{
  "topic": "orders",
  "partition": 0,
  "offset": 0
}
```

### Inspect Cluster State

Check broker health:

```bash
curl http://localhost:8083/cluster/brokers
```

Example:

```json
{
  "1": true,
  "2": true,
  "3": true
}
```

Inspect partition metadata:

```bash
curl http://localhost:8083/cluster/topics/orders/partitions/0
```

### Simulate Leader Failure

Stop Broker 1:

```bash
docker stop broker-1
```

After the heartbeat timeout:

```bash
curl http://localhost:8083/cluster/brokers
```

Broker 1 should be reported unavailable.

If Broker 1 was the leader for Partition 0, its replica can then become the new leader:

```bash
curl http://localhost:8083/cluster/topics/orders/partitions/0
```

Messages can continue to be produced using the surviving brokers.

---

## Tech Stack

- **Java 21**
- **Spring Boot**
- **Spring MVC**
- **Maven**
- **Docker / Docker Compose**
- File-based append-only logs
- REST-based broker communication

---

## Concepts Explored

This project was primarily built to understand the internals of distributed messaging systems.

Some of the concepts implemented include:

- Message queues vs event streams
- Topics and partitions
- Message keys and partition routing
- Ordered append-only logs
- Consumer offsets
- Consumer groups
- Partition assignment and rebalancing
- Consumer heartbeats
- Multi-broker clusters
- Leader/follower replication
- In-Sync Replicas (ISR)
- Producer acknowledgement semantics
- Broker heartbeats
- Failure detection
- Partition leader failover

---

## Simplifications

Distributed messaging systems are significantly more complex in production.

This project intentionally simplifies several areas to keep the implementation focused on the core architecture.

For example:

- Cluster membership is statically configured.
- Topic metadata is currently created independently on each broker.
- Broker coordination does not use ZooKeeper, Raft, or a dedicated controller quorum.
- ISR tracking is simplified using replica offsets.
- Leader election is deterministic and maintained locally by brokers.
- Failed replicas do not automatically catch up before rejoining.
- Broker-to-broker communication uses REST rather than a custom binary protocol.
- Exactly-once processing semantics are not implemented.

These trade-offs keep the project small enough to understand while still demonstrating the major building blocks of a distributed event broker.

---

## Why I Built This

The goal of this project was not to recreate Kafka feature-for-feature.

Instead, I wanted to understand what actually happens behind abstractions such as:

```text
producer.send(...)
consumer.poll(...)
```

Building the system from the ground up made concepts such as partition ownership, offsets, replication, acknowledgement guarantees, consumer rebalancing, failure detection, and leader failover much more concrete.