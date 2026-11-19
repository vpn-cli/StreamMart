# StreamMart Performance & Resilience Benchmarks

Comprehensive empirical performance benchmarks, resilience metrics, and failure recovery verifications for the StreamMart event-driven microservices architecture.

---

## 🖥️ Benchmark Environment & Test Rig

All benchmarks were measured directly on the local Docker orchestration environment under native host operating conditions:

| Specification | Hardware / Environment Value |
| :--- | :--- |
| **CPU Model** | AMD Ryzen 7 6800H with Radeon Graphics (8 physical cores, 16 logical threads @ 3.20 GHz base) |
| **Total Visible RAM** | 16.0 GB (16,004,012 KB visible memory) |
| **Host Operating System** | Microsoft Windows 11 Home Single Language (64-bit) |
| **Container Engine** | Docker version 29.9.3, build 5457199 (Docker Desktop with WSL2 Linux backend) |
| **Database Engine** | PostgreSQL 16 Alpine (`postgres:16-alpine`, containerized instance with persistent volume) |
| **Message Broker** | Apache Kafka 3.7.0 in KRaft Mode (`apache/kafka:3.7.0`, single-node KRaft, no ZooKeeper) |
| **Node.js Runtime** | Node.js v20.x (Docker base `node:20-alpine`) / Host Node.js v24.15.0 |
| **Load Testing Tool** | k6 v2.4.0 (commit/dd9395906d, go1.26.11, windows/amd64) |

---

## ⚡ 1. Cold Start Benchmark

Measures total wall-clock time required to orchestrate, create, boot, and verify health checks across all 7 microservices and infrastructure containers from a clean state (`docker compose down` followed by `docker compose up -d --wait`).

- **Command**:
  ```powershell
  Measure-Command { docker compose up -d --wait }
  ```
  *(Reproducible script: `benchmarks/cold-start.ps1`)*

- **Raw Execution Output**:
  ```text
  Network micro-services_default Created
  Container streammart-kafka Created
  Container streammart-postgres Created
  Container streammart-kafka-init Created
  Container streammart-payment Created
  Container streammart-analytic Created
  Container streammart-order Created
  Container streammart-email Created
  Container streammart-gateway Created
  Container streammart-postgres Healthy
  Container streammart-kafka Healthy
  Container streammart-kafka-init Exited (0)
  Container streammart-payment Healthy
  Container streammart-order Healthy
  Container streammart-email Healthy
  Container streammart-analytic Healthy
  Container streammart-gateway Healthy

  Days              : 0
  Hours             : 0
  Minutes           : 1
  Seconds           : 1
  Milliseconds      : 36
  TotalSeconds      : 61.0367579
  TotalMilliseconds : 61036.7579
  ```

- **Cold-Start Summary**:
  - **Total Elapsed Cold-Start Time**: **61.04 seconds** (all 7 containers created, started, topics provisioned, and passed healthcheck probes).

---

## 🚦 2. API Gateway Load Test (k6)

Measures the throughput, request latency distribution, and error rate of the API Gateway under steady concurrent load over 30 seconds with JWT authorization enforcement.

- **Command**:
  ```powershell
  k6 run benchmarks/gateway-load.js
  ```
- **Configuration**:
  - **Virtual Users (VUs)**: 20 concurrent VUs
  - **Duration**: 30 seconds steady state
  - **Endpoint**: `POST http://localhost:8085/api/payment`
  - **Auth**: Pre-authenticated Bearer JWT token in Authorization header
  - **Threshold**: Error rate < 1.0%

- **Raw k6 Output**:
  ```text
            /\      Grafana   /‾‾/  
       /\  /  \     |\  __   /  /   
      /  \/    \    | |/ /  /   ‾‾\ 
     /          \   |   (  |  (‾)  |
    / __________ \  |_|\_\  \_____/ 

        execution: local
           script: benchmarks/gateway-load.js
        scenarios: (100.00%) 1 scenario, 20 max VUs, 1m0s max duration (incl. graceful stop):
                 * default: 20 looping VUs for 30s (gracefulStop: 30s)

     █ THRESHOLDS 
       http_req_failed
       ✓ 'rate<0.01' rate=0.00%

     █ TOTAL RESULTS 
       checks_total.......: 2534    83.48115/s
       checks_succeeded...: 100.00% 2534 out of 2534
       checks_failed......: 0.00%   0 out of 2534

       ✓ status is 200

       HTTP
       http_req_duration..............: avg=180.34ms min=31.71ms med=153.62ms max=732.95ms p(90)=306.89ms p(95)=378.21ms
         { expected_response:true }...: avg=180.34ms min=31.71ms med=153.62ms max=732.95ms p(90)=306.89ms p(95)=378.21ms
       http_req_failed................: 0.00%  0 out of 2535
       http_reqs......................: 2535   83.514094/s

       EXECUTION
       iteration_duration.............: avg=237.37ms min=83.52ms med=209.06ms max=800.1ms  p(90)=367.8ms  p(95)=447.34ms
       iterations.....................: 2534   83.48115/s
       vus............................: 20     min=20        max=20
       vus_max........................: 20     min=20        max=20

       NETWORK
       data_received..................: 720 kB 24 kB/s
       data_sent......................: 1.2 MB 39 kB/s

   running (0m30.4s), 00/20 VUs, 2534 complete and 0 interrupted iterations
   default ✓ [ 100% ] 20 VUs  30s
  ```

- **Load Test Summary**:
  - **Throughput**: **83.51 requests/sec**
  - **Total Requests**: **2,535 requests**
  - **Error Rate**: **0.00%** (0 errors out of 2,535 requests)
  - **Minimum Latency**: 31.71 ms
  - **Median Latency (p50)**: **153.62 ms**
  - **Mean Latency**: 180.34 ms
  - **90th Percentile (p90)**: 306.89 ms
  - **95th Percentile (p95)**: **378.21 ms**
  - **Maximum Latency**: 732.95 ms

---

## ⏱️ 3. Latency Metrics: Synchronous Gateway & Asynchronous Event

Measures both the synchronous HTTP request overhead at the Gateway and the end-to-end distributed asynchronous event propagation latency across the entire microservices pipeline.

- **Command**:
  ```powershell
  node benchmarks/measure-latency.js
  ```

- **Raw Execution Output**:
  ```text
  ======================================================================
  📊 StreamMart Latency Benchmark: Synchronous Gateway & Asynchronous Event
  ======================================================================

  --- 1. Measuring Synchronous HTTP Gateway Latency ---
  First Request (Cold Path):      27.16 ms
  Steady-State Min:               19.07 ms
  Steady-State Max:               55.74 ms
  Steady-State Avg (Mean):        26.77 ms
  Steady-State p50 (Median):      25.64 ms
  Steady-State p95:               38.72 ms
  Steady-State p99:               55.74 ms

  --- 2. Measuring End-to-End Asynchronous Event Latency ---
  Definition: Time from order-service database commit (orders.created_at)
              to downstream email-service processing (processed_events.processed_at).

  Total Events Sampled:           4450
  End-to-End Event Latency Avg:   471.98 ms
  End-to-End Event Latency p50:   481.06 ms
  End-to-End Event Latency p95:   718.29 ms
  End-to-End Event Latency p99:   1292.58 ms
  ======================================================================
  ```

- **Summary**:
  - **Gateway Ingress Overhead (p50)**: **25.64 ms**
  - **Gateway Ingress Overhead (p95)**: **38.72 ms**
  - **End-to-End Event Propagation (p50)**: **481.06 ms**
  - **End-to-End Event Propagation (p95)**: **718.29 ms**
  - **End-to-End Event Propagation (p99)**: **1,292.58 ms**
  - **Dataset Verified**: 4,450 production transactions committed to PostgreSQL

---

## 🛡️ 4. Outbox Reliability & Broker Outage Benchmark

Measures system availability, zero data loss, and consumer idempotency when Apache Kafka suffers a total outage during peak checkout traffic, followed by dual concurrent outbox relay draining.

- **Command**:
  ```powershell
  node benchmarks/outbox-reliability.js
  ```

- **Test Steps**:
  1. Mint authenticated user JWTs via API Gateway.
  2. Stop Kafka container (`docker stop streammart-kafka`).
  3. Dispatch **500 concurrent checkout requests** to API Gateway (`POST /api/payment`).
  4. Assert 100% HTTP 200 acceptance and verify all 500 records are safely buffered in the transactional `outbox_events` table while Kafka is completely down.
  5. Restart Kafka container (`docker start streammart-kafka`).
  6. Launch **2 concurrent outbox relay worker instances** utilizing `SELECT ... FOR UPDATE SKIP LOCKED`.
  7. Wait for relay queue to drain and consumer to process all events.
  8. Execute strict database assertions for lost events and duplicate processing.

- **Raw Execution Output**:
  ```text
  ===========================================================================
  🧪 StreamMart Benchmark: Outbox Reliability Under Broker Outage
  ===========================================================================
  Scenario: Stop Kafka broker (docker stop streammart-kafka)
            Send 500 checkout requests through API Gateway.
            Restart Kafka broker, wait for Outbox Relay to drain.
            Assert: exactly 500 orders, 0 lost, 0 duplicates.

  [Step 1] Acquiring JWT token from API Gateway...
  ✅ Obtained JWT for batch: batch_1794938071336
  📊 Baseline DB State: 806 payments, 531 orders

  [Step 2] Stopping Kafka broker (docker stop streammart-kafka)...
  streammart-kafka
  🛑 Kafka broker stopped. Messaging system is completely OFFLINE.

  [Step 3] Dispatching 500 checkouts to API Gateway while Kafka is DOWN...
     Dispatched 100/500 checkouts...
     Dispatched 200/500 checkouts...
     Dispatched 300/500 checkouts...
     Dispatched 400/500 checkouts...
     Dispatched 500/500 checkouts...
  ✅ Dispatched 500/500 checkouts in 4.12s (100% accepted HTTP 200)

  [Step 4] Verifying database isolation while Kafka is OFFLINE:
     - New payments recorded in DB: 500/500
     - Pending outbox events in DB: 500/500
     - Orders processed while Kafka is down: 0 (Must be exactly 0)
  ✅ PASS: All 500 events safely buffered in outbox table without data loss.

  [Step 5] Restarting Kafka broker (docker start streammart-kafka)...
  streammart-kafka
  ⏳ Waiting for Kafka to become healthy and accept connections...
  ✅ Kafka broker is back ONLINE and HEALTHY.

  [Step 6] Running 2 concurrent Outbox Relay instances with SELECT ... FOR UPDATE SKIP LOCKED...
  ✅ Concurrent Outbox Relay Instance #2 launched in background.

  [Step 7] Waiting for outbox to drain and order-service to consume all events...
  ✅ Outbox completely drained and all orders processed in 32.3s!

  [Step 8] Strict Correctness & Concurrency Verification:
     1. Total orders created for batch: 500/500
        ✅ 0 Orders Lost (100% delivery guarantee).
     2. Duplicate orders detected: 0
        ✅ 0 Duplicates Processed (Strict Idempotency verified).
     3. Order-service processed_events records: 500/500
        ✅ Exactly 500 idempotent event records stored.

  ===========================================================================
  🎉 SUCCESS: 500 orders processed, 0 lost, 0 duplicates under broker outage & concurrent relays!
  ===========================================================================
  ```

- **Resilience Summary**:
  - **Orders Dispatched During Broker Outage**: 500
  - **Gateway Availability**: **100% (500 / 500 HTTP 200 OK)**
  - **Events Lost**: **0 (0.00%)**
  - **Duplicate Orders Processed**: **0 (0.00%)**
  - **Idempotency Guarantee**: **100% verified** via compound primary key `(event_id, service_name)` table `processed_events`.
  - **Relay Concurrency**: 2 independent worker instances executing `SELECT ... FOR UPDATE SKIP LOCKED` safely shared the outbox queue without deadlock or duplicate message publication.

---

## 💥 5. Service Crash-Recovery Benchmark (Dual-Write Elimination)

Empirically verifies that crashing `order-service` immediately after database commit (prior to network publish or Kafka offset commit) results in **zero lost events** and **zero duplicate processing**.

- **Command**:
  ```powershell
  node scripts/test-order-crash-recovery.js
  ```

- **Raw Execution Output**:
  ```text
  ===========================================================================
  🧪 StreamMart Phase C: Order-Service Crash Recovery Integration Test
  ===========================================================================
  Scenario: Kill order-service after DB commit but before event publish.
            Assert that upon restart, order-successful is emitted exactly once.

  [Step 1] Requesting JWT from API Gateway...
  ✅ Obtained JWT for: user_crash_1794938170436

  [Step 2] Sending Checkout with testCrashAfterDbCommit=true flag...
  ✅ Payment accepted by gateway (200 OK) and queued in outbox.

  [Step 3] Waiting for order-service to consume payment event and trigger crash...
  ✅ Confirmed: Order was committed to DB (Order ID: bdefa71b-6ced-4deb-9e48-187cee802b37, Status: processing)
  ✅ Confirmed DB Outbox state: 677fcc53-2b4d-46d4-8c83-669959a44e34|order-successful|f

  [Step 4] Checking order-service crash logs...
  💥 [TEST HOOK]: Simulating immediate order-service crash after DB commit (before relay publish)!

  [Step 5] Ensuring order-service is running...
  streammart-order
  ✅ order-service container restarted.

  [Step 6] Waiting for order-service outbox relay to publish order-successful...
  ✅ Confirmed in DB: outbox_events.published = true!

  [Step 7] Running Verification Assertions:
     - Total orders created for user: 1
     ✅ EXACTLY ONE order persisted in database (no duplicates).
     - Order outbox rows for this order: 1
     ✅ EXACTLY ONE order-successful outbox row created and published.
     - Email outbox rows for this order: 1
     ✅ EXACTLY ONE email-successful outbox row created and published.
     - Email service processed events: 1
     ✅ Email service successfully consumed and processed order-successful.

  ===========================================================================
  🎉 SUCCESS: Crash recovery test passed completely! Zero lost, zero duplicates.
  ===========================================================================
  ```

---

## 🔑 6. Architectural Topology & Partition Keys

To guarantee strict in-order processing and eliminate race conditions across distributed consumer partitions, deterministic partition keys are assigned without fallback chains:

| Topic | Partition Key | Justification & Ordering Guarantee |
| :--- | :--- | :--- |
| `payment-successful` | `userId` | Ensures all payment events for a specific user route to the exact same Kafka partition, preserving per-user financial event ordering. |
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
