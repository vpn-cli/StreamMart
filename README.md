# 🛒 StreamMart: Enterprise Microservices Architecture

<div align="center">
  <img src="https://img.shields.io/badge/Architecture-Event--Driven-blue.svg" alt="Architecture" />
  <img src="https://img.shields.io/badge/Message_Broker-Apache_Kafka-black.svg?logo=apachekafka" alt="Kafka" />
  <img src="https://img.shields.io/badge/Database-PostgreSQL_16-00E599.svg?logo=postgresql" alt="PostgreSQL" />
  <img src="https://img.shields.io/badge/ORM-Drizzle-C5F74F.svg?logo=drizzle" alt="Drizzle" />
  <img src="https://img.shields.io/badge/Reliability-Transactional_Outbox-orange.svg" alt="Outbox" />
  <img src="https://img.shields.io/badge/Orchestration-Docker_Compose-326CE5.svg?logo=docker" alt="Docker" />
</div>

## 📖 Overview
StreamMart is an enterprise-grade, event-driven e-commerce microservices platform built with Node.js, Apache Kafka, PostgreSQL, and Drizzle ORM. It showcases production-grade distributed systems patterns: **Transactional Outbox Pattern** with concurrent `SKIP LOCKED` worker relays, **At-Least-Once Delivery with Idempotency Deduplication** (`processed_events`), **Dead Letter Queues (DLQ)** with exponential backoff, **Deterministic Partition Keys**, and **Distributed Tracing Lineage** (`eventId`, `correlationId`, `causationId`).

---

## 🏗️ Architecture & Workflow

```mermaid
graph TD
    Client[Next.js Client UI] -->|JWT Auth & REST API| Gateway[API Gateway Port: 8085]
    
    Gateway -->|HTTP POST /api/payment| Payment[Payment Service]
    Gateway -->|HTTP GET/POST /api/orders| Order[Order Service]
    
    subgraph Payment Service (Atomic Outbox)
      Payment -->|1. Atomic SQL Tx: Payment + Outbox| DB[(PostgreSQL)]
      PaymentWorker[Outbox Relay Worker] -.->|2. SELECT FOR UPDATE SKIP LOCKED| DB
      PaymentWorker -->|3. Publish: payment-successful| Kafka[Apache Kafka]
    end
    
    subgraph Order Service (Dual-Write Elimination)
      Kafka -->|Topic: payment-successful| Order
      Order -->|1. Atomic SQL Tx: Order + ProcessedEvent + Outbox| DB
      OrderWorker[Order Outbox Relay] -.->|2. SELECT FOR UPDATE SKIP LOCKED| DB
      OrderWorker -->|3. Publish: order-successful| Kafka
    end

    subgraph Email Service (Idempotent Notification)
      Kafka -->|Topic: order-successful| Email[Email Service]
      Email -->|1. Dispatch SMTP Notification| Mailer[SMTP / Mail Gateway]
      Email -->|2. Atomic SQL Tx: ProcessedEvent + Outbox| DB
      EmailWorker[Email Outbox Relay] -.->|3. SELECT FOR UPDATE SKIP LOCKED| DB
      EmailWorker -->|4. Publish: email-successful| Kafka
    end

    subgraph Analytic Service (Auditing)
      Kafka -->|All Topics| Analytics[Analytic Service]
      Analytics -->|Idempotent Event Log Commit| DB
    end
```

---

## 🛠️ Technology Stack
- **API Gateway**: Node.js, Express, `http-proxy-middleware`, Centralized JWT Verification, Header Normalization
- **Microservices**: Node.js (Payment, Order, Email, Analytic)
- **Message Broker**: Apache Kafka 7.5.0 & Zookeeper
- **Database & Persistence**: PostgreSQL 16 (Neon Serverless & Dockerized Postgres) with connection pooling
- **ORM & Migrations**: Drizzle ORM (`drizzle-orm`, `drizzle-kit`)
- **Testing & Quality Assurance**: Vitest, Jest, Supertest, Custom Resilience & Chaos Test Harnesses
- **Containerization**: Docker, Docker Compose (`docker-compose.yml`)

---

## 💡 Production Architecture Patterns

### 1. Dual-Write Elimination (Transactional Outbox Pattern)
- **Problem**: Writing to PostgreSQL and directly calling Kafka's `producer.send()` creates a distributed dual-write vulnerability. If the network drops or the container crashes after the database commit, the event is permanently lost; re-delivery triggers idempotency checks and skips.
- **Solution**: Every event producer (`payment-service`, `order-service`, `email-service`) performs all mutations and records outgoing events into an `outbox_events` table within a **single atomic database transaction**. Independent background relay workers poll `outbox_events` using `SELECT ... FOR UPDATE SKIP LOCKED` and publish to Kafka, completely eliminating race conditions and event loss.

### 2. Distributed Event Lineage (`eventId`, `correlationId`, `causationId`)
Every event emitted across the cluster contains standardized distributed tracing metadata:
- `eventId`: Unique UUIDv4 identifying this specific message instance.
- `correlationId`: Root transaction UUID (originating payment event ID) that persists across the entire lifecycle for full auditability.
- `causationId`: Immediate parent event UUID that directly triggered this message.
- Consumers strictly dedupe against the event's own `eventId`.

### 3. Idempotent Consumers Everywhere (`processed_events`)
- All downstream consumers (`order-service`, `email-service`, `analytic-service`) maintain a `processed_events` table.
- When an event is consumed, the consumer checks if `eventId` already exists.
- **Concurrent Duplicate Handling**: If two consumer instances process duplicate events concurrently, the loser hits PostgreSQL error code `23505` (primary key unique violation). The consumer catches `23505` and cleanly skips the message without triggering retries or routing to the Dead Letter Queue.

### 4. Deterministic Partition Keys
To ensure strict per-entity ordering without partition skew or fallback chain bugs, deterministic partition keys are enforced:
- `payment-successful` -> Key: `userId` (preserves per-user transaction ordering)
- `order-successful` -> Key: `orderId` (preserves per-order lifecycle mutations)
- `email-successful` -> Key: `orderId` (preserves order notification sequencing)
- `payment-successful.DLQ` -> Key: `eventId` (even distribution of failed events)

### 5. Email Idempotency Trade-Off
For `email-service`, recording `processed_events` **after** email dispatch was intentionally chosen:
- If recorded *before*, a container crash during SMTP dispatch permanently drops the email.
- If recorded *after*, a container crash immediately post-send might send a duplicate email upon restart.
- In e-commerce, an occasional duplicate order confirmation is strictly preferable to a lost confirmation.

---

## 📊 Empirical Benchmarks & Resilience

See [BENCHMARKS.md](file:///D:/resume%20project/devops/micro-services/BENCHMARKS.md) for full benchmark logs, system specs, and percentile distributions.

| Metric | Empirical Value |
| :--- | :--- |
| **Cold Start (Full Cluster)** | **29.42 seconds** (`docker compose down` -> `docker compose up -d --wait`) |
| **Gateway Ingress Latency (p50)** | **31.97 ms** (Mean: 36.63 ms, p95: 74.95 ms) |
| **End-to-End Event Latency (p50)** | **338.50 ms** (Min: 269.07 ms, Mean: 405.15 ms, p95: 704.95 ms) |
| **Kafka Outage Tolerance (500 reqs)** | **100% HTTP 200** accepted in 3.66s; 0 lost, 0 duplicate orders |
| **Concurrent Outbox Drain (2 relays)** | **500 events drained in 14.10s** via `SKIP LOCKED` |
| **Crash-Recovery Guarantee** | Kill after DB commit before publish -> **0 lost, 0 duplicates, exactly 1 emitted** |

---

## 🚀 Running Locally

### 1. Configure Environment
```bash
cp .env.template .env
# Edit .env with your PostgreSQL credentials (or use the docker-compose postgres service)
```

### 2. Run Test Suites
```bash
# Run all unit and integration test suites
npm test

# Run resilience test suites
node scripts/test-order-crash-recovery.js
node scripts/test-outbox-outage.js
node scripts/measure-e2e-latency.js
```

### 3. Spin Up Docker Cluster
```bash
docker compose up --build -d
```
