# Phase 1 Benchmark Results — StreamMart

> [!IMPORTANT]
> **Status of Historical Results**:
> - **Pre-Clean-Rerun Results (initial drifted-environment runs & earlier `pool_keepalive_optimized` run)**: **SUPERSEDED** — ran without database resets, accumulated unpruned outbox rows, and did not drain Kafka consumer lag.
> - **Clean Matrix Rerun (`benchmarks/run-benchmarks.js`, Section 5 below)**:
>   - All **"Completed checkouts/s"** and **"Drain Time"** values in the Clean Matrix are **SUPERSEDED** because that script only waited for `outbox_events` backlog = 0, not Kafka consumer lag = 0 (`orders == accepted payments`), allowing consumer lag to carry across runs.
>   - The **`pool_keepalive_batched` row** in the Clean Matrix is **SUPERSEDED / INVALID** because `RELAY_BATCH_SEND` was not passed through `docker-compose.yml` (defaulted to `false` inside the container, so it actually ran sequential 1-by-1 sends identical to `pool_keepalive`).
>   - The **25ms busy-pause rebound claim** (`102 -> 228 req/s`) is **SUPERSEDED / CONFOUNDED** (see Section 2).
>   - The **Accepted req/s, p50, and p95** for `baseline`, `pool_only`, `keepalive_only`, and `pool_keepalive` remain **VALID** for ingress under sequential relay sends + 25ms busy pause (with the caveat that consumer lag carried over between runs).
> - **Authoritative End-to-End Results**: Section 1 (`baseline_consumer`, `fix_a`, `fix_b`) contains the fully verified, zero-leftover-lag, full-consumer-drain measurements with container-logged `[EFFECTIVE CONFIG]` captured at the start of every experiment.

**Test Host**: Local laptop — AMD Ryzen 7 6800H, 8 physical cores / 16 logical threads @ 3.20 GHz, 16 GB RAM, Windows 11 Home (64-bit), Docker Desktop (WSL2 backend). All 7 containers + k6 runner on the same host machine.

**Authoritative Raw Data Sources** (in `benchmarks/raw/`):
- `summary-experiment-baseline_consumer.json` · `summary-experiment-fix_a.json` · `summary-experiment-fix_b.json`
- Per-run k6 JSON summaries & raw stdout captures (`baseline_consumer-*`, `fix_a-*`, `fix_b-*`)
- `consumer-bottleneck-evidence.json` (`benchmarks/measure-consumer-breakdown.mjs`)
- `master-gateway-keepalive-measurement.json` (`benchmarks/measure-master-gateway.mjs`)
- Historical clean matrix raw files: `summary-baseline.json`, `summary-pool_only.json`, `summary-keepalive_only.json`, `summary-pool_keepalive.json`, `summary-pool_keepalive_batched.json` (partially superseded; see Section 2 & 5)

---

## 1. Authoritative Consumer Optimization Experiments (`pool_keepalive_batched`, Full Drain to Lag = 0)

**Methodology (`benchmarks/run-consumer-experiment.js`)**:
1. **Clean Reset**: Before each experiment, all application containers are stopped, Kafka topics (`payment-successful`, `order-successful`, `email-successful`, `payment-successful.DLQ`) are deleted, waited 4s for broker cleanup, recreated with the exact target partition count, and verified via `kafka-topics.sh --describe`. PostgreSQL is truncated (`TRUNCATE payments, orders, processed_events, outbox_events CASCADE;`) and all three topic-scoped partial outbox indexes are verified.
2. **Container Config Verification**: Containers are started with `docker compose up -d --build --force-recreate`. Startup `[EFFECTIVE CONFIG]` log lines are read directly from inside each running container and persisted into each experiment's summary JSON.
3. **Zero Leftover Lag Guarantee**: Before every measured run, the harness asserts `SELECT COUNT(*) FROM payments == SELECT COUNT(*) FROM orders` (zero leftover consumer lag).
4. **Full End-to-End Drain**: After each 30s k6 window (1,000 distinct user JWTs cycled across VUs and iterations), the harness snapshots consumer lag at 30s (`acceptedPayments - ordersDuring30s`), then polls PostgreSQL until `ordersProcessed == acceptedPayments` (Kafka consumer lag = 0, timeout 300s). True completed throughput is:
   $$\text{True Completed Checkouts/s} = \frac{\text{Completed Orders}}{\text{Run Duration} + \text{Catch-up Time to Lag } 0}$$
5. **Protocol**: 1 discarded 30s warm-up (fully drained to lag = 0) + 3 measured 30s runs per tier (20, 50, 100 VUs). **All 27 measured runs completed with 0% HTTP errors and `timedOut = false`.**

---

### Experiment A: `baseline_consumer` (1 Partition, Sequential `eachMessage`)

**Effective Container Startup Config (verified from inside each container)**:
```text
Kafka payment-successful: PartitionCount: 1, ReplicationFactor: 1
streammart-gateway: [EFFECTIVE CONFIG] api-gateway: PORT=8080, PAYMENT_SERVICE_URL=http://payment-service:8000, PROXY_KEEP_ALIVE=true, NODE_ENV=development
streammart-payment: [EFFECTIVE CONFIG] payment-service: PORT=8000, DB_POOL_MAX=30, RELAY_BATCH_SEND=true, RELAY_BUSY_PAUSE_MS=25, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
streammart-order:   [EFFECTIVE CONFIG] order-service: PORT=8001, DB_POOL_MAX=30, CONSUMER_MODE=eachMessage, PARTITIONS_CONSUMED_CONCURRENTLY=1, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
streammart-email:   [EFFECTIVE CONFIG] email-service: PORT=8002, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
```

**Results — Mean [Min – Max] (3 measured runs per tier)**:

| Tier | Accepted req/s | True Completed checkouts/s | Consumer Lag @ 30s (msgs) | Catch-up Time to Lag 0 (s) | p50 Latency (ms) | p95 Latency (ms) |
|---|---|---|---|---|---|---|
| **20 VUs** | 199.06 [140.43 – 232.05] | 71.37 [51.75 – 93.17] | 3,984 [3,156 – 4,428] | 57.57 [46.23 – 69.89] | 41.49 [29.50 – 65.33] | 104.31 [68.41 – 174.21] |
| **50 VUs** | 227.17 [182.26 – 250.19] | 56.23 [54.49 – 57.45] | 5,645 [4,365 – 6,311] | 96.11 [70.60 – 112.58] | 135.36 [126.36 – 152.62] | 350.41 [252.59 – 544.59] |
| **100 VUs** | 285.61 [259.49 – 314.85] | 48.42 [45.66 – 50.73] | 7,420 [6,789 – 8,265] | 154.33 [129.96 – 171.42] | 245.14 [211.40 – 272.66] | 496.65 [457.90 – 568.14] |

---

### Experiment B: `fix_a` (6 Partitions + `partitionsConsumedConcurrently: 6`, `eachMessage`)

**Effective Container Startup Config (verified from inside each container)**:
```text
Kafka payment-successful: PartitionCount: 6, ReplicationFactor: 1
streammart-gateway: [EFFECTIVE CONFIG] api-gateway: PORT=8080, PAYMENT_SERVICE_URL=http://payment-service:8000, PROXY_KEEP_ALIVE=true, NODE_ENV=development
streammart-payment: [EFFECTIVE CONFIG] payment-service: PORT=8000, DB_POOL_MAX=30, RELAY_BATCH_SEND=true, RELAY_BUSY_PAUSE_MS=25, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
streammart-order:   [EFFECTIVE CONFIG] order-service: PORT=8001, DB_POOL_MAX=30, CONSUMER_MODE=eachMessage, PARTITIONS_CONSUMED_CONCURRENTLY=6, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
streammart-email:   [EFFECTIVE CONFIG] email-service: PORT=8002, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
```

**Results — Mean [Min – Max] (3 measured runs per tier)**:

| Tier | Accepted req/s | True Completed checkouts/s | Consumer Lag @ 30s (msgs) | Catch-up Time to Lag 0 (s) | p50 Latency (ms) | p95 Latency (ms) |
|---|---|---|---|---|---|---|
| **20 VUs** | 220.04 [209.89 – 230.66] | 152.24 [144.10 – 163.05] | 2,142 [1,585 – 2,686] | 14.06 [8.91 – 18.63] | 33.44 [29.13 – 38.52] | 75.09 [69.09 – 79.73] |
| **50 VUs** | 273.48 [251.92 – 284.94] | 90.74 [86.22 – 95.73] | 5,858 [4,900 – 6,525] | 63.70 [50.73 – 73.40] | 111.33 [96.08 – 133.23] | 218.66 [205.88 – 240.92] |
| **100 VUs** | 340.17 [320.31 – 361.23] | 87.88 [71.32 – 119.92] | 7,912 [6,854 – 9,077] | 97.92 [57.14 – 128.01] | 205.64 [196.75 – 215.20] | 399.67 [370.53 – 429.09] |

---

### Experiment C: `fix_b` (1 Partition + `eachBatch`, 1 SQL Transaction per Batch)

Per-user FIFO ordering is preserved (`key = userId` on a single partition; batch messages are processed in Kafka offset order inside a single atomic SQL transaction with bulk `inArray` idempotency check against `processed_events`, bulk `INSERT` into `orders`, `processed_events` (`ON CONFLICT DO NOTHING`), and `outbox_events`, followed by `resolveOffset()` after commit).

**Effective Container Startup Config (verified from inside each container)**:
```text
Kafka payment-successful: PartitionCount: 1, ReplicationFactor: 1
streammart-gateway: [EFFECTIVE CONFIG] api-gateway: PORT=8080, PAYMENT_SERVICE_URL=http://payment-service:8000, PROXY_KEEP_ALIVE=true, NODE_ENV=development
streammart-payment: [EFFECTIVE CONFIG] payment-service: PORT=8000, DB_POOL_MAX=30, RELAY_BATCH_SEND=true, RELAY_BUSY_PAUSE_MS=25, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
streammart-order:   [EFFECTIVE CONFIG] order-service: PORT=8001, DB_POOL_MAX=30, CONSUMER_MODE=eachBatch, PARTITIONS_CONSUMED_CONCURRENTLY=1, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
streammart-email:   [EFFECTIVE CONFIG] email-service: PORT=8002, OUTBOX_RETENTION_HOURS=1, KAFKA_BROKERS=kafka:9092
```

**Results — Mean [Min – Max] (3 measured runs per tier)**:

| Tier | Accepted req/s | True Completed checkouts/s | Consumer Lag @ 30s (msgs) | Catch-up Time to Lag 0 (s) | p50 Latency (ms) | p95 Latency (ms) |
|---|---|---|---|---|---|---|
| **20 VUs** | 253.12 [246.76 – 260.03] | **251.17 [244.84 – 258.08]** | **0 [0 – 0]** | **0.24 [0.23 – 0.24]** | **21.77 [20.29 – 22.52]** | **55.91 [52.51 – 58.74]** |
| **50 VUs** | 372.13 [335.84 – 391.51] | **368.92 [333.01 – 388.35]** | **0 [0 – 0]** | **0.27 [0.25 – 0.29]** | **74.00 [68.02 – 85.40]** | **140.81 [125.59 – 169.14]** |
| **100 VUs** | 379.49 [354.89 – 399.75] | **375.75 [349.95 – 396.79]** | **0 [0 – 0]** | **0.31 [0.23 – 0.44]** | **178.47 [169.93 – 188.84]** | **374.79 [340.02 – 393.71]** |

### Side-by-Side Comparison Summary (Means)

| Tier | Metric | `baseline_consumer` (1 part, `eachMessage`) | `fix_a` (6 parts, `concurrency=6`) | `fix_b` (1 part, `eachBatch`) | `fix_b` vs `baseline` |
|---|---|---|---|---|---|
| **20 VUs** | True Completed checkouts/s | 71.37 | 152.24 (+2.13x) | **251.17** | **+3.52x** |
| **20 VUs** | Consumer Lag @ 30s / Catch-up | 3,984 msgs / 57.57s | 2,142 msgs / 14.06s | **0 msgs / 0.24s** | **-100% lag** |
| **50 VUs** | True Completed checkouts/s | 56.23 | 90.74 (+1.61x) | **368.92** | **+6.56x** |
| **50 VUs** | Consumer Lag @ 30s / Catch-up | 5,645 msgs / 96.11s | 5,858 msgs / 63.70s | **0 msgs / 0.27s** | **-100% lag** |
| **100 VUs** | True Completed checkouts/s | 48.42 | 87.88 (+1.81x) | **375.75** | **+7.76x** |
| **100 VUs** | Consumer Lag @ 30s / Catch-up | 7,420 msgs / 154.33s | 7,912 msgs / 97.92s | **0 msgs / 0.31s** | **-100% lag** |

**Key Architectural Insight**:
- **`fix_a` (6 partitions + concurrent `eachMessage`)** improves throughput ~1.6x–2.1x over 1-partition `eachMessage`, but still plateaus under 100 orders/s at 50–100 VUs because every message executes its own PostgreSQL transaction (`BEGIN` + `SELECT` + 3 `INSERT`s + `COMMIT` + WAL fsync), creating write contention on Postgres.
- **`fix_b` (`eachBatch`, 1 transaction per batch)** collapses dozens of per-message transactions into a single SQL transaction per Kafka batch. Even on **1 single partition**, consumer throughput catches up to full ingress (**375.75 completed checkouts/s at 100 VUs**, **0 lag**), AND eliminating hundreds of tiny `order-service` transactions/s reduces Postgres contention enough that **accepted ingress throughput also rises** (+32.9% at 100 VUs: 285.61 → 379.49 req/s, and p50 drops from 245 ms → 178 ms).

---

## 2. Missing Environment Variable Audit & Validity of Earlier Results

Prior to commit `5c7f584`, five environment variables were read in application code but omitted from `docker-compose.yml` service `environment:` blocks.

| Missing Env Var | Service(s) | Exact Code Default When Unset | Effect Before Commit `5c7f584` |
|---|---|---|---|
| `CONSUMER_MODE` | `order-service` | `"eachMessage"` (`order-service/index.js:118`) | Always ran `eachMessage` (single-message mode); `eachBatch` could never activate via Compose. |
| `PARTITIONS_CONSUMED_CONCURRENTLY` | `order-service` | `1` (`parseInt(... \|\| "1", 10)`, `order-service/index.js:117`) | Always consumed 1 partition sequentially even on multi-partition topics. |
| `RELAY_BATCH_SEND` | `payment-service` | `false` (`process.env.RELAY_BATCH_SEND === "true"`, `payment-service/worker.js:130`) | **Always fell into the `else` branch (sequential 1-by-1 `producer.send`)** in all post-`e90c881` runs! |
| `RELAY_BUSY_PAUSE_MS` | `payment-service` | `25` ms (`parseInt(... \|\| "25", 10)`, `payment-service/worker.js:208`) | Always paused 25ms between non-empty relay batches regardless of host env overrides. |
| `OUTBOX_RETENTION_HOURS` | `payment-service`, `order-service`, `email-service` | `1` hour (`parseInt(... \|\| "1", 10)` in each `worker.js`) | Always used 1-hour retention (also note `payment-service` pruner SQL failed silently due to raw JS `Date` parameter until fixed in `5c7f584`). |

### Validity Assessment of Earlier Results

| Earlier Result / Experiment | Status | Exact Reason |
|---|---|---|
| **Pre-clean-rerun runs & earlier `"optimized"` run (`summary-pool_keepalive_optimized.json`)** | **SUPERSEDED** | Ran in a drifted environment (no Postgres truncation, unpruned tables) and did not drain Kafka consumer lag. Note: before `e90c881`, batched Kafka sends were hardcoded ON with 0ms busy pause, so that earlier run *did* exercise batched sends + 0ms pause, but in a drifted DB. |
| **Clean Matrix — `pool_keepalive_batched` row** (`summary-pool_keepalive_batched.json`) | **SUPERSEDED / INVALID** | `RELAY_BATCH_SEND` was missing from `docker-compose.yml` and defaulted to `false` inside the container, so this row actually executed **sequential** sends (duplicate of `pool_keepalive`). Also lacked full consumer lag drain. Replaced by Section 1 (`baseline_consumer`, `fix_a`, `fix_b`) with verified `RELAY_BATCH_SEND=true`. |
| **Clean Matrix — `baseline`, `pool_only`, `keepalive_only`, `pool_keepalive` (Accepted req/s, p50, p95)** | **PARTIALLY VALID (Ingress Only)** | `DB_POOL_MAX` and `PROXY_KEEP_ALIVE` *were* passed in `docker-compose.yml`, and sequential relay (`RELAY_BATCH_SEND` unset = `false`) matched their intended config. However, their **Completed checkouts/s** and **Drain Time** columns are **SUPERSEDED** because `run-benchmarks.js` only drained `outbox_events` (not Kafka consumer lag) and allowed consumer lag to carry across runs. |
| **25ms Busy-Pause Result (`102 -> 228 req/s` rebound at 20 VUs)** | **SUPERSEDED / CONFOUNDED (Observed Effect Only)** | The earlier `102 req/s` run had batched sends + 0ms pause in a drifted DB, whereas the `228 req/s` clean-matrix run accidentally had **sequential sends (`RELAY_BATCH_SEND=false`)** + 25ms pause in a clean DB. Because multiple variables changed simultaneously and no event-loop lag or PostgreSQL lock waits were measured with 0ms vs 25ms in isolation, the 25ms pause cannot be claimed as a proven causal mechanism. Under verified `RELAY_BATCH_SEND=true` + `RELAY_BUSY_PAUSE_MS=25` in Section 1, 20 VU accepted throughput is **199–253 req/s**, confirming that batched sends with the 25ms pause do not starve checkouts — treated strictly as an **observed effect**. |
| **Master Branch Gateway Keep-Alive Probe (`benchmarks/measure-master-gateway.mjs`)** | **VALID** | Measured directly on checked-out `master` branch `api-gateway`: 4,396 HTTP requests → 4,405 TCP ActiveOpens (141.57 opens/sec, ~1:1 ratio, proving original gateway did NOT reuse connections). |
| **Consumer Bottleneck Breakdown (`benchmarks/measure-consumer-breakdown.mjs`)** | **VALID** | Direct per-message timing inside `order-service` `eachMessage` (Section 3). |
| **Native JWT Micro-benchmark (`benchmarks/measure-jwt.js`)** | **VALID** | Measured `jwt.verify()` at 15.2 µs – 82.4 µs (mean **21.9 µs**). |

---

## 3. Consumer Bottleneck Empirical Evidence (`order-service`)

To isolate why single-partition `eachMessage` consumption plateaued far below ingress, high-resolution `performance.now()` instrumentation was added to `order-service/index.js` (`GET /consumer-stats`) and sampled over 500 messages via `benchmarks/measure-consumer-breakdown.mjs` (`benchmarks/raw/consumer-bottleneck-evidence.json`):

| Phase inside `eachMessage` | Low-Contention Sample (N=500, commit `40a760d`) | Under-Load Sample (N=500, `consumer-bottleneck-evidence.json`) | Share of Total |
|---|---|---|---|
| **Atomic DB Transaction** (`orders` + `processed_events` + `outbox_events`) | avg **10.77 ms** (p50 8.92 ms, p95 22.06 ms) | avg **22.10 ms** (p50 17.14 ms, p95 50.54 ms) | **80.3% – 91.2%** |
| **Idempotency `SELECT`** (`processed_events`) | avg **2.40 ms** (p50 1.86 ms, p95 4.62 ms) | avg **2.04 ms** (p50 1.62 ms, p95 3.35 ms) | **8.4% – 17.9%** |
| **CPU / JSON Parse / Overhead** | avg **0.25 ms** (p50 0.23 ms, p95 0.38 ms) | avg **0.08 ms** (p50 0.08 ms, p95 0.12 ms) | **0.3% – 1.9%** |
| **Total per Message** | avg **13.42 ms** (p50 **11.07 ms**) | avg **24.22 ms** (p50 **19.29 ms**) | **100%** |
| **Implied 1-Partition Ceiling** | **74.5 – 90.3 msgs/s** | **41.3 – 51.8 msgs/s** | — |

**Conclusion**: **98.2% – 99.7%** of `order-service` message processing time is PostgreSQL I/O (not Node.js CPU or JSON parsing). Under sequential 1-partition `eachMessage` processing, each message incurs its own transaction commit latency (~11–24 ms), capping throughput at ~48–90 orders/s depending on concurrent DB write load — closely matching our measured `baseline_consumer` true completed rates (71.37/s at 20 VUs, 56.23/s at 50 VUs, 48.42/s at 100 VUs).

---

## 4. Shared Outbox Backlog, Per-Topic Indexes & Real-Postgres Pruner Verification

### Backlog Composition & Architectural Choice
All three transactional services (`payment-service`, `order-service`, `email-service`) share the `outbox_events` table in PostgreSQL, distinguished by the `topic` column (`payment-successful`, `order-successful`, `email-successful`). Previously, only `payment-successful` had a partial index and pruning logic; published rows for `order-successful` and `email-successful` accumulated unbounded (>93,000 rows each during extended runs), causing full table scans on those workers' relay queries.

**Choice — Shared Table with Per-Topic Partial Indexes & Scoped Pruning (vs. Separate Tables per Service)**:
We retained the single `outbox_events` table and added dedicated partial indexes + scoped pruners for each topic:
- `idx_outbox_events_payment_unpublished`: `(created_at ASC) WHERE published = false AND topic = 'payment-successful'`
- `idx_outbox_events_order_unpublished`: `(created_at ASC) WHERE published = false AND topic = 'order-successful'`
- `idx_outbox_events_email_unpublished`: `(created_at ASC) WHERE published = false AND topic = 'email-successful'`
- Each service's `worker.js` runs its own topic-scoped pruner (`DELETE FROM outbox_events WHERE published = true AND topic = '<own-topic>' AND created_at < NOW() - INTERVAL '1 hour' * $1`), eliminating cross-service deletion risk.
- **Why this choice for Phase 1**: Preserves atomic local DB transactions across all three services without introducing schema migrations/multiple database instances yet, while giving each service an isolated O(unpublished) index and bounded table size. (In a future production microservice split with separate database-per-service instances, each service would naturally own its own `outbox_events` table.)

### Real-Postgres Outbox Pruner Integration Test (`scripts/test-outbox-pruner.js`)
Executed against live `streammart-postgres` (no mocks):
- Inserted 6 rows: 2 old published `payment-successful` (2–3h old), 1 recent published `payment-successful` (10m old), 1 old **unpublished** `payment-successful` (2h old, `published = false`), 1 old published `order-successful` (2h old), and 1 old published `email-successful` (2h old).
- Ran real `prunePublishedEvents(true)`, `pruneOrderOutboxEvents(1)`, and `pruneEmailOutboxEvents(1)` inside each service image:
  - Deleted 2 `payment-successful`, 1 `order-successful`, and 1 `email-successful` old published rows.
  - Retained both the recent published row (10m old) and the old **unpublished** row (`published = false`).
- Pruner errors now log loudly (`🚨 [CRITICAL][Outbox Pruner] PRUNE FAILED`) and are exposed in `GET /relay-status` (`pruner.lastError`, `pruner.consecutiveErrors`, `pruner.lastRunAt`, `pruner.lastDeletedCount`; verified in unit tests `payment-service/tests/payment.test.js` 7/7 passing and live `/relay-status` probe).

---

## 5. Split Health Checks & Relay Startup Resilience Verification

- **`GET /health` (Liveness)**: Always returns **200 OK** while the `payment-service` HTTP process is alive. Used by `docker-compose.yml` healthcheck and `api-gateway` `depends_on` so Kafka outages NEVER restart `payment-service` or block checkout ingestion.
- **`GET /relay-status` (Relay & Pruner Readiness)**: Returns **503 Service Unavailable** (JSON) when the Kafka outbox relay loop is disconnected, and **200 OK** (JSON with `status: "ok"` or `"degraded"` if `pruner.lastError` is non-null) when connected. Not referenced by any `docker-compose.yml` `depends_on` or restart policy.

**Verbatim Output (`node scripts/test-relay-startup.js`)**:
```text
===========================================================================
🧪 Relay Startup Integration Test: Kafka Down -> Startup -> Up -> Deliver
===========================================================================
Test user: user_startup_1795012091881
[Step 1] Ensuring clean database and Kafka state for test isolation...
TRUNCATE TABLE
   Baseline DB State: 0 payments, 0 orders

[Step 2] Stopping Kafka broker (docker stop streammart-kafka)...
🛑 Kafka broker stopped.

[Step 3] Restarting payment-service with Kafka DOWN (docker restart streammart-payment)...

[Step 4] Probing payment-service /health and /relay-status while Kafka is down...
   Liveness (/health) response: {"status":200,"text":"Payment Service OK"}
   Relay Status (/relay-status) response: {"status":503,"text":"{\"status\":\"unhealthy\",\"message\":\"Payment Service Relay Loop Not Running\",\"relayRunning\":false,\"kafkaConnected\":false,\"pruner\":{\"lastRunAt\":null,\"lastDeletedCount\":0,\"lastError\":null,\"lastErrorAt\":null,\"consecutiveErrors\":0}}"}
✅ PASS: /health returned 200 (process alive) and /relay-status returned 503 (relay loop not connected to Kafka).

[Step 5] Submitting checkout to payment-service while Kafka is down...
   Payment response: {"status":200,"text":"Payment processed and event safely queued in outbox."}
✅ PASS: Payment accepted (HTTP 200) and atomically queued in outbox table.

[Step 6] Verifying database isolation while Kafka is down...
   - Unpublished outbox row for user: 1 (Expected: 1)
   - Orders created for user while Kafka down: 0 (Expected: 0)
✅ PASS: Event safely buffered in outbox table with published = false.

[Step 7] Bringing Kafka UP (docker start streammart-kafka)...
✅ Kafka broker is back ONLINE and HEALTHY.

[Step 8] Waiting for payment relay retry loop to connect and /relay-status to return 200...
   /relay-status returned 200 OK after 1s! Message: "{"status":"ok","message":"Payment Service Relay Running","relayRunning":true,"kafkaConnected":true,"pruner":{"lastRunAt":"2026-11-18T14:29:01.299Z","lastDeletedCount":0,"lastError":null,"lastErrorAt":null,"consecutiveErrors":0}}"
✅ PASS: /relay-status successfully transitioned to 200 OK upon Kafka reconnection.

[Step 9] Confirming event delivery and downstream order processing...
✅ PASS: Event published to Kafka (published = true) and order-service created order for user_startup_1795012091881!

===========================================================================
🎉 SUCCESS: Relay startup test complete! Kafka connect retry with backoff,
   503 health check under outage, and zero-loss event delivery verified!
===========================================================================
```

---

## 6. Historical Clean Matrix Archive (Partially Superseded — Retained for Reference)

> [!WARNING]
> **Superseded Notice**:
> - The **`pool_keepalive_batched`** row below is **SUPERSEDED / INVALID** (ran with `RELAY_BATCH_SEND=false` because `docker-compose.yml` did not forward it).
> - All **Completed checkouts/s** and **Drain Time** columns below are **SUPERSEDED** (measured outbox drain only, not Kafka consumer lag = 0; runs started with leftover consumer lag).
> - All **pre-clean-rerun** numbers (from earlier un-reset runs) are **SUPERSEDED** and removed from active tables.
> - **Accepted req/s, p50, and p95** for `baseline`, `pool_only`, `keepalive_only`, and `pool_keepalive` remain **VALID** for ingress under sequential relay sends + 25ms pause.

### Accepted Throughput (accepted req/s) — mean [min – max]

| Configuration | Validity | 20 VUs | 50 VUs | 100 VUs |
|---|---|---|---|---|
| `baseline` (`DB_POOL_MAX=10`, `PROXY_KEEP_ALIVE=false`, seq relay) | Valid (ingress) | 152.81 [135.17 – 170.40] | 154.50 [115.15 – 176.64] | 138.39 [103.39 – 182.07] |
| `pool_only` (`DB_POOL_MAX=30`, `PROXY_KEEP_ALIVE=false`, seq relay) | Valid (ingress) | 186.46 [175.11 – 192.31] | 212.06 [199.87 – 222.66] | 214.61 [205.41 – 222.26] |
| `keepalive_only` (`DB_POOL_MAX=10`, `PROXY_KEEP_ALIVE=true`, seq relay) | Valid (ingress) | 225.29 [200.28 – 238.85] | 316.84 [309.33 – 325.95] | 327.18 [320.60 – 332.06] |
| `pool_keepalive` (`DB_POOL_MAX=30`, `PROXY_KEEP_ALIVE=true`, seq relay) | Valid (ingress) | 231.37 [226.94 – 234.56] | 310.49 [275.76 – 337.35] | 329.52 [314.51 – 339.80] |
| ~~`pool_keepalive_batched`~~ | **SUPERSEDED** (`RELAY_BATCH_SEND` unset = `false`) | ~~228.35 [217.51 – 237.44]~~ | ~~337.78 [336.77 – 339.37]~~ | ~~332.69 [320.88 – 340.20]~~ |

### Latency p50 / p95 (ms) — mean [min – max]

| Configuration | Validity | 20 VUs (p50 / p95) | 50 VUs (p50 / p95) | 100 VUs (p50 / p95) |
|---|---|---|---|---|
| `baseline` | Valid (ingress) | 57.85 / 152.93 | 219.11 / 425.59 | 479.24 / 1,356.51 |
| `pool_only` | Valid (ingress) | 45.36 / 87.19 | 163.20 / 236.29 | 384.15 / 493.28 |
| `keepalive_only` | Valid (ingress) | 29.07 / 60.68 | 94.78 / 150.17 | 230.61 / 323.27 |
| `pool_keepalive` | Valid (ingress) | 30.75 / 58.89 | 92.10 / 140.21 | 221.04 / 389.39 |
| ~~`pool_keepalive_batched`~~ | **SUPERSEDED** | ~~31.83 / 59.24~~ | ~~88.85 / 131.21~~ | ~~215.13 / 381.28~~ |

### Historical Completed Checkouts/s & Outbox Drain (ALL ROWS SUPERSEDED — Replaced by Section 1)

| Configuration | Status | 20 VUs (Completed/s) | 50 VUs (Completed/s) | 100 VUs (Completed/s) |
|---|---|---|---|---|
| ~~`baseline`~~ | **SUPERSEDED** (no consumer lag drain) | ~~71.04 [64.02 – 82.82]~~ | ~~62.09 [43.19 – 74.24]~~ | ~~61.39 [47.15 – 82.66]~~ |
| ~~`pool_only`~~ | **SUPERSEDED** (no consumer lag drain) | ~~89.48 [86.33 – 95.07]~~ | ~~88.92 [82.39 – 96.43]~~ | ~~89.13 [84.04 – 94.27]~~ |
| ~~`keepalive_only`~~ | **SUPERSEDED** (no consumer lag drain) | ~~103.60 [84.90 – 114.05]~~ | ~~104.54 [100.12 – 111.92]~~ | ~~106.40 [102.80 – 113.45]~~ |
| ~~`pool_keepalive`~~ | **SUPERSEDED** (no consumer lag drain) | ~~105.22 [104.09 – 107.24]~~ | ~~104.94 [97.15 – 111.20]~~ | ~~106.38 [101.11 – 114.52]~~ |
| ~~`pool_keepalive_batched`~~ | **SUPERSEDED** (`RELAY_BATCH_SEND=false` + no consumer lag drain) | ~~106.62 [100.00 – 117.30]~~ | ~~106.64 [102.87 – 113.70]~~ | ~~106.37 [101.43 – 114.21]~~ |

---

## 7. Validated Micro-Benchmarks & Architectural Findings (Retained)

1. **Original Master Branch Gateway Keep-Alive (`benchmarks/measure-master-gateway.mjs`)**: Under 20 VUs (30s), original `master` `api-gateway` proxied 4,396 HTTP requests and opened 4,405 outbound TCP connections (**141.57 TCP opens/sec**, ~1:1 ratio). The original gateway did **not** reuse connections because omitting `agent` in `http-proxy-middleware` defaults to `agent: false`.
2. **Native JWT Verification (`benchmarks/measure-jwt.js`)**: `jwt.verify()` takes **15.2 µs – 82.4 µs (mean: 21.9 µs)** in Node.js.
3. **Shared Outbox Deletion Impact**: Because `payment-service`, `order-service`, and `email-service` share `outbox_events`, the earlier manual `DELETE WHERE topic != 'payment-successful'` permanently deleted `order-successful` and `email-successful` events (both pending notifications and historical audit rows). All pruning is now strictly topic-scoped (`AND topic = '<own-topic>' AND published = true`).
