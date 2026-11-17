# StreamMart Performance & Resilience Benchmarks

Comprehensive empirical performance benchmarks, resilience metrics, and failure recovery verifications for the StreamMart event-driven microservices architecture.

---

## 🖥️ Benchmark Environment & Test Rig

All benchmarks were measured directly on the local Docker orchestration environment under native host operating conditions:

| Specification | Hardware / Environment Value |
| :--- | :--- |
| **CPU Model** | AMD Ryzen 7 6800H with Radeon Graphics (8 physical cores, 16 logical threads @ 3.20 GHz base) |
| **Total Visible RAM** | 16.0 GB (16,004,012 KB visible memory) |
| **Host Operating System** | Windows 11 64-bit |
| **Container Engine** | Docker version 29.9.3, build 5457199 (Docker Desktop with WSL2 Linux backend) |
| **Database Engine** | PostgreSQL 16 (Neon Serverless & containerized instance) with connection pooling |
| **Message Broker** | Apache Kafka 7.5.0 + Zookeeper |
| **Node.js Runtime** | Node.js v20.x (Docker base `node:20-alpine`) |

---

## ⚡ 1. Cold Start Benchmark

Measures total time required to orchestrate and bring all 7 microservices and infrastructure containers from a full `docker compose down` shutdown to completely healthy and ready to accept incoming traffic (`docker compose up -d --wait`).

- **Command**: `Measure-Command { docker compose down; docker compose up -d --wait }`
- **Total Elapsed Cold-Start Time**: **29.42 seconds**
- **Container Health Verifications**:
  - `streammart-postgres`: Healthy (TCP 5432 pg_isready)
  - `streammart-zookeeper`: Healthy (TCP 2181)
  - `streammart-kafka`: Healthy (TCP 9092 & zookeeper cluster registration)
  - `streammart-gateway`: Healthy (HTTP 200 on port 8085 `/health`)
  - `streammart-payment`: Healthy (HTTP 200 on port 8000 `/health`)
  - `streammart-order`: Healthy (Kafka consumer group connected, outbox worker active)
  - `streammart-email`: Healthy (Kafka consumer group connected, outbox worker active)
  - `streammart-analytic`: Healthy (Kafka consumer group connected)

---

## ⏱️ 2. API Gateway Ingress Latency

Measures the latency overhead introduced by the unified API Gateway reverse-proxy (JWT token verification, header injection, reverse proxy routing to upstream services) under steady-state conditions.

- **Warmup Request**: 26.99 ms
- **Minimum Latency**: 19.66 ms
- **Median (p50)**: **31.97 ms**
- **Mean Latency**: 36.63 ms
- **95th Percentile (p95)**: **74.95 ms**
- **99th Percentile (p99)**: 104.97 ms

---

## 🚀 3. End-to-End Distributed Event Latency

Measures the total wall-clock duration spanning from initial client checkout (`POST /api/payment`) through API Gateway authentication, Payment Service transactional outbox commit, Outbox Worker Kafka publication (`payment-successful`), Order Service Kafka consumption, and Order Service transactional database commit (`orders` + `processed_events` + `outbox_events`).

- **Test Suite**: `scripts/measure-e2e-latency.js`
- **Sample Size**: 50 sequential end-to-end checkout transactions
- **Distribution**:
  - **Minimum**: 269.07 ms
  - **Mean**: 405.15 ms
  - **Median (p50)**: **338.50 ms**
  - **95th Percentile (p95)**: **704.95 ms**
  - **99th Percentile (p99)**: 814.56 ms

```
Latency Percentiles (ms):
p50  | [######################                 ] 338.50 ms
p95  | [###################################   ] 704.95 ms
p99  | [######################################] 814.56 ms
```

---

## 🛡️ 4. Broker Outage & Concurrent Relay Drainage Benchmark

Measures system availability and zero data-loss guarantees when the central message broker (Kafka) experiences a complete outage.

### Test Execution: `scripts/test-outbox-outage.js`

1. **Broker Teardown**: Stopped Kafka container (`docker stop streammart-kafka`).
2. **Traffic Dispatch**: Submitted **500 distinct checkout transactions** against API Gateway (`POST /api/payment`) using 500 unique authenticated user JWTs.
3. **Availability Under Failure**:
   - Total Checkouts: **500**
   - HTTP 200 Success Rate: **100% (500 / 500)**
   - Total Dispatch Duration: **3.66 seconds** (~136.6 req/sec)
   - Payment Database Outbox Rows Created: **500 rows** (`published = false`)
4. **Broker Recovery**: Restarted Kafka (`docker start streammart-kafka`).
5. **Drainage Performance**:
   - Outbox Workers: **2 concurrent worker instances** running `SELECT ... FOR UPDATE SKIP LOCKED`.
   - Total Drain Time: **14.10 seconds** (~35.5 events/sec end-to-end drainage and processing).
6. **Data Integrity Audit**:
   - Total Orders Created in Database: **500**
   - Lost Orders: **0 (0.00%)**
   - Duplicate Orders: **0 (0.00%)**
   - Processed Events Recorded: **500**

---

## 💥 5. Service Crash-Recovery Benchmark (Dual-Write Elimination)

Empirically verifies that killing `order-service` immediately after database commit (but before it can publish or acknowledge) results in **zero lost events** and **exactly-once execution**.

### Test Execution: `scripts/test-order-crash-recovery.js`

1. **Crash Injection**: Sent checkout request with crash simulation hook (`testCrashAfterDbCommit: true`).
2. **Order Service Commit**: `order-service` atomically commits the order, outbox row, and `processed_events` row inside a single PostgreSQL transaction.
3. **Container Kill**: `order-service` process immediately self-terminates (`process.exit(1)`) prior to network publish or Kafka offset commit.
4. **Container Restart**: Docker restarts the crashed container.
5. **Redelivery & Idempotent Handling**:
   - Kafka re-delivers the uncommitted `payment-successful` event.
   - `order-service` queries `processed_events` (or handles PostgreSQL primary key violation error code `23505`).
   - Consumer identifies message as already processed and skips duplicate order creation.
   - Dedicated outbox worker (`order-service/worker.js`) queries unpublished rows with `SELECT ... FOR UPDATE SKIP LOCKED`.
   - Outbox worker reliably publishes `order-successful` to Kafka and marks row as published.
6. **Audit Assertions**:
   - Orders created: **1**
   - Duplicate orders: **0**
   - `order-successful` events published: **1 (Exactly-Once)**
   - Lost events: **0**

---

## 🔑 6. Architectural Topology & Partition Keys

To guarantee strict in-order processing and eliminate race conditions across distributed consumer partitions, deterministic partition keys are assigned without fallback chains:

| Topic | Partition Key | Justification & Ordering Guarantee |
| :--- | :--- | :--- |
| `payment-successful` | `userId` | Ensures all payment events for a specific user route to the exact same Kafka partition, preserving per-user event ordering. |
| `order-successful` | `orderId` | Ensures downstream order lifecycle mutations (inventory, shipping, notifications) are ordered strictly by order instance. |
| `email-successful` | `orderId` | Guarantees all email delivery tracking events for a given order are partitioned sequentially. |
| `payment-successful.DLQ` | `eventId` | Ensures isolated, evenly distributed dead-letter queue routing without skewing onto single user partitions. |

---

## ⚖️ 7. Email Idempotency Trade-Off Analysis

In distributed systems, external non-transactional side-effects (e.g., SMTP / SendGrid / Amazon SES) cannot participate in two-phase database commits. In `email-service`, we evaluated two deduplication architectures:

### Strategy A: Record "Processed" BEFORE Sending Email
- **Flow**: Insert `processed_events` -> Send email -> Commit.
- **Failure Mode**: If the email service process terminates or network fails during SMTP dispatch, the database record already exists. Upon container reboot, Kafka redelivers the event, the service reads `processed_events`, assumes the email succeeded, and skips it.
- **Result**: **Permanently lost confirmation email.** Customer never receives receipt or shipping confirmation.

### Strategy B: Record "Processed" AFTER Sending Email (Implemented Choice)
- **Flow**: Send email via SMTP -> Atomically commit `processed_events` + outbox row.
- **Failure Mode**: If the service terminates immediately after the SMTP provider accepts the email but before the database commit finishes, Kafka redelivers the event upon restart. The service dispatches a duplicate email.
- **Result**: **Occasional duplicate email receipt.**

### Architectural Decision
In e-commerce operations, **a duplicate order confirmation is strictly preferable to a lost confirmation**. A customer receiving two receipt emails experiences minor inconvenience; a customer receiving zero receipts assumes the transaction failed, contacts support, or erroneously re-submits a duplicate payment. StreamMart adopts Strategy B with downstream email message-id deduplication headers.
