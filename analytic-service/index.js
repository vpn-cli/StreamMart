import http from "node:http";
import { Kafka } from "kafkajs";
import { db } from "./db/index.js";
import { processedEvents } from "./db/schema.js";
import { and, eq } from "drizzle-orm";
import { processAnalyticBatch } from "./consumer-processor.js";

let isReady = false;
let isShuttingDown = false;
let inflightMessages = 0;

// Health check server for container orchestration
const healthServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    if (isReady && !isShuttingDown) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Analytic Service OK");
    } else {
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end(isShuttingDown ? "Analytic Service Shutting Down" : "Analytic Service Starting...");
    }
  } else {
    res.writeHead(404);
    res.end();
  }
});

const HEALTH_PORT = process.env.PORT || 8003;
healthServer.listen(HEALTH_PORT, () => {
  const effectivePoolMax = parseInt(process.env.DB_POOL_MAX || "10", 10);
  const effectiveConsumerMode = process.env.ANALYTIC_CONSUMER_MODE || process.env.CONSUMER_MODE || "eachBatch";
  console.log(`🚀 Analytic service health server listening on port ${HEALTH_PORT}`);
  console.log(
    `[EFFECTIVE CONFIG] analytic-service: PORT=${HEALTH_PORT}, DB_POOL_MAX=${effectivePoolMax}, CONSUMER_MODE=${effectiveConsumerMode}, KAFKA_BROKERS=${process.env.KAFKA_BROKERS || "localhost:9092"}`
  );
});

const kafka = new Kafka({
  clientId: "analytic-service",
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

const consumer = kafka.consumer({ groupId: "analytic-service" });

const run = async () => {
  let connected = false;
  while (!connected && !isShuttingDown) {
    try {
      await consumer.connect();
      await consumer.subscribe({
        topics: ["payment-successful", "order-successful", "email-successful"],
        fromBeginning: true,
      });
      connected = true;
      isReady = true;
      console.log("Analytic Service: Connected to Kafka and subscribed to topics");
    } catch (err) {
      console.warn("Analytic Service: Retrying Kafka connection/subscription in 2s...", err.message);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  if (isShuttingDown) return;

  const consumerMode = process.env.ANALYTIC_CONSUMER_MODE || process.env.CONSUMER_MODE || "eachBatch";
  const tables = { processedEvents };
  console.log(`Analytic Service: Running consumer with mode=${consumerMode}`);

  if (consumerMode === "eachBatch") {
    await consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
        if (!isRunning() || isStale()) return;
        inflightMessages += batch.messages.length;
        try {
          await processAnalyticBatch({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary,
            isRunning,
            isStale,
            db,
            tables,
          });
        } finally {
          inflightMessages = Math.max(0, inflightMessages - batch.messages.length);
        }
      },
    });
  } else {
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        inflightMessages++;
        try {
          const value = message.value.toString();
          const data = JSON.parse(value);
          const { eventId, correlationId, causationId } = data;

          // 1. Consumer Idempotency Check: dedupe on incoming event's own eventId
          if (db && eventId) {
            try {
              const existing = await db
                .select()
                .from(processedEvents)
                .where(
                  and(
                    eq(processedEvents.eventId, String(eventId)),
                    eq(processedEvents.serviceName, "analytic-service")
                  )
                )
                .limit(1);

              if (existing && existing.length > 0) {
                console.warn(`Analytic Service [Idempotency]: Event ID ${eventId} already processed by analytic-service. Skipping duplicate metric aggregation.`);
                return;
              }
            } catch (e) {
              console.error("Analytic Service: Error querying processed_events:", e.message);
            }
          }

          // 2. Aggregate / log event metrics
          switch (topic) {
            case "payment-successful": {
              const { userId, cart } = data;
              const total = cart && Array.isArray(cart)
                ? cart.reduce((acc, item) => acc + (Number(item.price) || 0), 0).toFixed(2)
                : "0.00";
              console.log(`Analytic consumer [payment-successful]: User ${userId} paid $${total} (Event: ${eventId}, Corr: ${correlationId || eventId})`);
              break;
            }
            case "order-successful": {
              const { userId, orderId } = data;
              console.log(`Analytic consumer [order-successful]: Order ${orderId} confirmed for user ${userId} (Event: ${eventId}, Corr: ${correlationId}, Caus: ${causationId})`);
              break;
            }
            case "email-successful": {
              const { userId, emailId, orderId } = data;
              console.log(`Analytic consumer [email-successful]: Email ${emailId} dispatched to user ${userId} for order ${orderId} (Event: ${eventId})`);
              break;
            }
            default:
              console.log(`Analytic consumer [${topic}]: Received event ${eventId}`);
              break;
          }

          // 3. Atomically Record Idempotency in processed_events
          if (db && eventId) {
            try {
              await db.insert(processedEvents).values({
                eventId: String(eventId),
                serviceName: "analytic-service",
              });
            } catch (dbErr) {
              if (
                dbErr.code === "23505" ||
                dbErr.message?.includes("duplicate key") ||
                dbErr.message?.includes("processed_events_pkey") ||
                dbErr.message?.includes("unique constraint")
              ) {
                console.warn(`Analytic Service [Idempotency]: Unique constraint violation for event ID ${eventId}. Concurrent duplicate detected; skipping cleanly.`);
                return;
              }

              console.error("Analytic Service: Failed to insert into processed_events:", dbErr.message);
            }
          }
        } finally {
          inflightMessages = Math.max(0, inflightMessages - 1);
        }
      },
    });
  }
};

run().catch((err) => {
  console.error("Analytic Service Kafka error:", err);
});

// Graceful shutdown
const shutdown = async () => {
  if (isShuttingDown) return;
  isShuttingDown = true;
  isReady = false;
  console.log("Analytic Service: SIGTERM received. Initiating graceful shutdown...");

  healthServer.close();

  const maxWait = 5000;
  const start = Date.now();
  while (inflightMessages > 0 && Date.now() - start < maxWait) {
    await new Promise((r) => setTimeout(r, 200));
  }

  try {
    await consumer.disconnect();
    console.log("Analytic Service: Consumer disconnected.");
  } catch (err) {
    console.error("Analytic Service error during disconnect:", err.message);
  }

  console.log("Analytic Service: Graceful shutdown complete. Exiting cleanly.");
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
