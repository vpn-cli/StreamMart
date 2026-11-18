import http from "node:http";
import crypto from "node:crypto";
import { performance } from "node:perf_hooks";
import { Kafka } from "kafkajs";
import { db } from "./db/index.js";
import { orders, outboxEvents, processedEvents } from "./db/schema.js";
import { and, eq, inArray } from "drizzle-orm";
import { startOrderOutboxWorker, stopOrderOutboxWorker } from "./worker.js";

let isReady = false;
let isShuttingDown = false;
let inflightMessages = 0;

// High-resolution timing metrics buffer
const timingMetrics = {
  total: [],
  idempotency: [],
  tx: [],
  overhead: [],
};

export function getConsumerStats() {
  const calc = (arr) => {
    if (arr.length === 0) return { avg: 0, p50: 0, p95: 0, min: 0, max: 0 };
    const sorted = [...arr].sort((a, b) => a - b);
    const avg = arr.reduce((a, b) => a + b, 0) / arr.length;
    const p50 = sorted[Math.floor(sorted.length * 0.5)];
    const p95 = sorted[Math.floor(sorted.length * 0.95)];
    const min = sorted[0];
    const max = sorted[sorted.length - 1];
    return {
      avg: Number(avg.toFixed(2)),
      p50: Number(p50.toFixed(2)),
      p95: Number(p95.toFixed(2)),
      min: Number(min.toFixed(2)),
      max: Number(max.toFixed(2)),
    };
  };

  const total = calc(timingMetrics.total);
  const idempotency = calc(timingMetrics.idempotency);
  const tx = calc(timingMetrics.tx);
  const overhead = calc(timingMetrics.overhead);

  return {
    sampleCount: timingMetrics.total.length,
    total,
    idempotency,
    tx,
    overhead,
    impliedThroughput: total.avg > 0 ? Number((1000 / total.avg).toFixed(2)) : 0,
  };
}

// Health check server for container orchestration & metrics
const healthServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    if (isReady && !isShuttingDown) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Order Service OK");
    } else {
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end(isShuttingDown ? "Order Service Shutting Down" : "Order Service Starting...");
    }
  } else if (req.url === "/consumer-stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(getConsumerStats(), null, 2));
  } else {
    res.writeHead(404);
    res.end();
  }
});

const HEALTH_PORT = process.env.PORT || 8001;
healthServer.listen(HEALTH_PORT, () => {
  console.log(`🚀 Order service health server listening on port ${HEALTH_PORT}`);
});

const kafka = new Kafka({
  clientId: "order-service",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
  ...(process.env.KAFKA_SASL_USERNAME && {
    ssl: true,
    sasl: {
      mechanism: "plain",
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    },
  }),
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "order-service" });

const run = async () => {
  let connected = false;
  while (!connected && !isShuttingDown) {
    try {
      await producer.connect();
      await consumer.connect();
      await consumer.subscribe({
        topic: "payment-successful",
        fromBeginning: true,
      });
      await startOrderOutboxWorker();
      connected = true;
      isReady = true;
      console.log("Order Service: Connected to Kafka and subscribed to payment-successful");
    } catch (err) {
      console.warn("Order Service: Retrying Kafka connection/subscription in 2s...", err.message);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  if (isShuttingDown) return;

  const partitionsConsumedConcurrently = parseInt(process.env.PARTITIONS_CONSUMED_CONCURRENTLY || "1", 10);
  const consumerMode = process.env.CONSUMER_MODE || "eachMessage"; // "eachMessage" or "eachBatch"
  console.log(`Order Service: Running consumer with mode=${consumerMode}, partitionsConsumedConcurrently=${partitionsConsumedConcurrently}`);

  if (consumerMode === "eachBatch") {
    await consumer.run({
      partitionsConsumedConcurrently,
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        if (!isRunning() || isStale()) return;
        inflightMessages += batch.messages.length;
        const t0 = performance.now();
        let tIdemp = 0;
        let tTx = 0;

        try {
          const parsedItems = [];
          for (const message of batch.messages) {
            try {
              const raw = message.value.toString();
              const data = JSON.parse(raw);
              parsedItems.push({
                message,
                data,
                eventId: String(data.eventId || ""),
                userId: String(data.userId || ""),
                cart: data.cart || [],
                correlationId: data.correlationId,
                causationId: data.causationId,
              });
            } catch (parseErr) {
              console.error("Order Service [JSON Error]:", parseErr.message);
              resolveOffset(message.offset);
            }
          }

          if (parsedItems.length === 0) return;

          // 1. Bulk Idempotency check with IN query
          const eventIds = parsedItems.map((p) => p.eventId).filter(Boolean);
          let existingSet = new Set();
          if (process.env.DATABASE_URL && eventIds.length > 0) {
            const tIdemp0 = performance.now();
            try {
              const existing = await db
                .select({ eventId: processedEvents.eventId })
                .from(processedEvents)
                .where(
                  and(
                    inArray(processedEvents.eventId, eventIds),
                    eq(processedEvents.serviceName, "order-service")
                  )
                );
              existingSet = new Set(existing.map((e) => e.eventId));
            } catch (e) {
              console.error("Order Service: Bulk idempotency check failed:", e.message);
            }
            tIdemp = performance.now() - tIdemp0;
          }

          // Filter out duplicates
          const newItems = parsedItems.filter((p) => !existingSet.has(p.eventId));

          // 2. Single SQL transaction for the entire batch of new orders
          if (newItems.length > 0 && process.env.DATABASE_URL) {
            const tTx0 = performance.now();
            await db.transaction(async (tx) => {
              // Bulk insert orders
              const insertedOrders = await tx
                .insert(orders)
                .values(
                  newItems.map((item) => ({
                    userId: item.userId,
                    items: item.cart,
                    status: "processing",
                  }))
                )
                .returning();

              // Bulk insert processed_events
              await tx
                .insert(processedEvents)
                .values(
                  newItems.map((item) => ({
                    eventId: item.eventId,
                    serviceName: "order-service",
                  }))
                )
                .onConflictDoNothing();

              // Bulk insert outbox_events
              await tx.insert(outboxEvents).values(
                newItems.map((item, idx) => {
                  const orderId = insertedOrders[idx]?.id;
                  const orderEventId = crypto.randomUUID();
                  const rootCorrelationId = String(item.correlationId || item.eventId || orderEventId);
                  const directCausationId = String(item.eventId || rootCorrelationId);
                  return {
                    id: orderEventId,
                    topic: "order-successful",
                    payload: {
                      eventId: orderEventId,
                      correlationId: rootCorrelationId,
                      causationId: directCausationId,
                      orderId,
                      userId: item.userId,
                      items: item.cart,
                    },
                    published: false,
                  };
                })
              );
            });
            tTx = performance.now() - tTx0;
          }

          // 3. Mark offsets as resolved
          for (const item of parsedItems) {
            resolveOffset(item.message.offset);
          }
          await heartbeat();

          const tTotal = performance.now() - t0;
          const tOverhead = Math.max(0, tTotal - tIdemp - tTx);
          const perMsgTotal = tTotal / parsedItems.length;
          const perMsgIdemp = tIdemp / parsedItems.length;
          const perMsgTx = tTx / parsedItems.length;
          const perMsgOverhead = tOverhead / parsedItems.length;

          for (let i = 0; i < parsedItems.length; i++) {
            timingMetrics.total.push(perMsgTotal);
            timingMetrics.idempotency.push(perMsgIdemp);
            timingMetrics.tx.push(perMsgTx);
            timingMetrics.overhead.push(perMsgOverhead);
          }

          if (timingMetrics.total.length % 200 < parsedItems.length) {
            const stats = getConsumerStats();
            console.log(
              `📊 [Order eachBatch N=${stats.sampleCount} msgs] Total avg=${stats.total.avg}ms/msg | DB Tx avg=${stats.tx.avg}ms/msg | Implied Rate=${stats.impliedThroughput} msgs/s`
            );
          }
        } finally {
          inflightMessages = Math.max(0, inflightMessages - batch.messages.length);
        }
      },
    });
  } else {
    // Sequential eachMessage processing (default mode)
    await consumer.run({
      partitionsConsumedConcurrently,
      eachMessage: async ({ topic, partition, message }) => {
        inflightMessages++;
        const t0 = performance.now();
        let tIdemp = 0;
        let tTx = 0;
        try {
          const rawValue = message.value.toString();
          const data = JSON.parse(rawValue);
          const { userId, cart, eventId, correlationId, causationId } = data;

          // 1. Idempotency Check: if eventId has already been processed by order-service, skip side-effect
          if (process.env.DATABASE_URL && eventId) {
            const tIdemp0 = performance.now();
            try {
              const existing = await db
                .select()
                .from(processedEvents)
                .where(
                  and(
                    eq(processedEvents.eventId, String(eventId)),
                    eq(processedEvents.serviceName, "order-service")
                  )
                )
                .limit(1);

              tIdemp = performance.now() - tIdemp0;
              if (existing && existing.length > 0) {
                console.warn(`Order Service [Idempotency]: Event ID ${eventId} already processed by order-service. Skipping duplicate order creation.`);
                return;
              }
            } catch (e) {
              tIdemp = performance.now() - tIdemp0;
              console.error("Order Service: Error querying processed_events:", e.message);
            }
          }

          // 2. Atomic Order Creation + Outbox Event + Idempotency Record in ONE SQL transaction
          let orderId = null;
          let isDuplicateViolation = false;
          const MAX_RETRIES = 3;
          let attempt = 0;

          if (process.env.DATABASE_URL) {
            while (attempt < MAX_RETRIES) {
              attempt++;
              try {
                // Generate fresh eventId for downstream order-successful event
                const orderEventId = crypto.randomUUID();
                const rootCorrelationId = String(correlationId || eventId || orderEventId);
                const directCausationId = String(eventId || rootCorrelationId);

                const tTx0 = performance.now();
                await db.transaction(async (tx) => {
                  const inserted = await tx
                    .insert(orders)
                    .values({
                      userId: String(userId),
                      items: cart || [],
                      status: "processing",
                    })
                    .returning();

                  if (inserted && inserted.length > 0) {
                    orderId = inserted[0].id;
                  }

                  if (eventId) {
                    await tx.insert(processedEvents).values({
                      eventId: String(eventId),
                      serviceName: "order-service",
                    });
                  }

                  // Outbox pattern: write order-successful outbox row in SAME transaction
                  await tx.insert(outboxEvents).values({
                    id: orderEventId,
                    topic: "order-successful",
                    payload: {
                      eventId: orderEventId,
                      correlationId: rootCorrelationId,
                      causationId: directCausationId,
                      orderId,
                      userId,
                      items: cart || [],
                    },
                    published: false,
                  });
                });
                tTx = performance.now() - tTx0;

                // Succeeded
                break;
              } catch (dbErr) {
                if (
                  dbErr.code === "23505" ||
                  dbErr.message?.includes("duplicate key") ||
                  dbErr.message?.includes("processed_events_pkey") ||
                  dbErr.message?.includes("unique constraint")
                ) {
                  console.warn(`Order Service [Idempotency]: Unique constraint violation for event ID ${eventId}. Concurrent duplicate detected; skipping cleanly.`);
                  isDuplicateViolation = true;
                  break;
                }

                console.error(`Order Service: DB write failed on attempt ${attempt}/${MAX_RETRIES}:`, dbErr.message);
                if (attempt < MAX_RETRIES) {
                  await new Promise((resolve) => setTimeout(resolve, 100 * Math.pow(2, attempt - 1)));
                }
              }
            }
          } else {
            orderId = `order_${Date.now()}`;
          }

          if (isDuplicateViolation) {
            return;
          }

          if (!orderId) {
            console.error(`Order Service [POISON MESSAGE]: Failed to create order after ${MAX_RETRIES} attempts. Routing to payment-successful.DLQ`);
            await producer.send({
              topic: "payment-successful.DLQ",
              messages: [
                {
                  key: String(eventId || userId || "unknown"),
                  value: JSON.stringify({
                    originalPayload: data,
                    error: `Exhausted ${MAX_RETRIES} DB insert retries`,
                    failedAt: new Date().toISOString(),
                    eventId,
                  }),
                  headers: {
                    "error-reason": "DB_INSERTION_EXHAUSTED",
                    "original-event-id": String(eventId || ""),
                  },
                },
              ],
            });
            return;
          }

          if (process.env.TEST_CRASH_AFTER_DB_COMMIT === "true" || data.testCrashAfterDbCommit === true) {
            console.warn("💥 [TEST HOOK]: Simulating immediate order-service crash after DB commit (before relay publish)!");
            process.exit(99);
          }

          const tTotal = performance.now() - t0;
          const tOverhead = Math.max(0, tTotal - tIdemp - tTx);
          timingMetrics.total.push(tTotal);
          timingMetrics.idempotency.push(tIdemp);
          timingMetrics.tx.push(tTx);
          timingMetrics.overhead.push(tOverhead);

          if (timingMetrics.total.length % 100 === 0) {
            const stats = getConsumerStats();
            console.log(`📊 [Order Consumer Breakdown N=${stats.sampleCount}] Total=${stats.total.avg}ms | DB Tx=${stats.tx.avg}ms (${((stats.tx.avg / stats.total.avg) * 100).toFixed(1)}%) | Idempotency SELECT=${stats.idempotency.avg}ms (${((stats.idempotency.avg / stats.total.avg) * 100).toFixed(1)}%) | CPU/Overhead=${stats.overhead.avg}ms | Implied Rate=${stats.impliedThroughput} msgs/s`);
          }
        } finally {
          inflightMessages = Math.max(0, inflightMessages - 1);
        }
      },
    });
  }
};

run().catch((err) => {
  console.error("Order Service Kafka error:", err);
});

// Graceful shutdown
const shutdown = async () => {
  if (isShuttingDown) return;
  isShuttingDown = true;
  isReady = false;
  console.log("Order Service: SIGTERM received. Initiating graceful shutdown...");

  // Stop health check server
  healthServer.close();

  // Wait for any in-flight messages to complete processing (up to 5 seconds)
  const maxWait = 5000;
  const start = Date.now();
  while (inflightMessages > 0 && Date.now() - start < maxWait) {
    console.log(`Order Service: Waiting for ${inflightMessages} inflight message(s) to complete...`);
    await new Promise((r) => setTimeout(r, 200));
  }

  try {
    await stopOrderOutboxWorker();
    console.log("Order Service: Outbox worker stopped.");
    await consumer.disconnect();
    console.log("Order Service: Consumer disconnected.");
    await producer.disconnect();
    console.log("Order Service: Producer disconnected.");
  } catch (err) {
    console.error("Order Service: Error during Kafka disconnect:", err.message);
  }

  console.log("Order Service: Graceful shutdown complete. Exiting cleanly.");
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
