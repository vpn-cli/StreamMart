import http from "node:http";
import crypto from "node:crypto";
import { Kafka } from "kafkajs";
import { db } from "./db/index.js";
import { outboxEvents, processedEvents } from "./db/schema.js";
import { and, eq } from "drizzle-orm";
import { startEmailOutboxWorker, stopEmailOutboxWorker } from "./worker.js";

let isReady = false;
let isShuttingDown = false;
let inflightMessages = 0;

// Health check server for container orchestration
const healthServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    if (isReady && !isShuttingDown) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Email Service OK");
    } else {
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end(isShuttingDown ? "Email Service Shutting Down" : "Email Service Starting...");
    }
  } else {
    res.writeHead(404);
    res.end();
  }
});

const HEALTH_PORT = process.env.PORT || 8002;
healthServer.listen(HEALTH_PORT, () => {
  const effectiveRetentionHours = parseInt(process.env.OUTBOX_RETENTION_HOURS || "1", 10);
  console.log(`🚀 Email service health server listening on port ${HEALTH_PORT}`);
  console.log(
    `[EFFECTIVE CONFIG] email-service: PORT=${HEALTH_PORT}, OUTBOX_RETENTION_HOURS=${effectiveRetentionHours}, KAFKA_BROKERS=${process.env.KAFKA_BROKERS || "localhost:9092"}`
  );
});

const kafka = new Kafka({
  clientId: "email-service",
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
const consumer = kafka.consumer({ groupId: "email-service" });

const run = async () => {
  let connected = false;
  while (!connected && !isShuttingDown) {
    try {
      await producer.connect();
      await consumer.connect();
      await consumer.subscribe({
        topic: "order-successful",
        fromBeginning: true,
      });
      await startEmailOutboxWorker();
      connected = true;
      isReady = true;
      console.log("Email Service: Connected to Kafka and subscribed to order-successful");
    } catch (err) {
      console.warn("Email Service: Retrying Kafka connection/subscription in 2s...", err.message);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  if (isShuttingDown) return;

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      inflightMessages++;
      try {
        const value = message.value.toString();
        const data = JSON.parse(value);
        const { userId, orderId, eventId, correlationId, causationId } = data;

        // 1. Consumer Idempotency Check: dedupe on incoming event's own eventId
        if (db && eventId) {
          try {
            const existing = await db
              .select()
              .from(processedEvents)
              .where(
                and(
                  eq(processedEvents.eventId, String(eventId)),
                  eq(processedEvents.serviceName, "email-service")
                )
              )
              .limit(1);

            if (existing && existing.length > 0) {
              console.warn(`Email Service [Idempotency]: Event ID ${eventId} already processed by email-service. Skipping duplicate email dispatch.`);
              return;
            }
          } catch (e) {
            console.error("Email Service: Error querying processed_events:", e.message);
          }
        }

        // 2. Perform External Side-Effect (Sending Email)
        // Architectural Decision (Item 3):
        // We record "processed" AFTER sending the email rather than before.
        // Trade-off: If we recorded "processed" BEFORE sending, a crash during email dispatch
        // causes a permanent MISSED EMAIL (the customer never receives confirmation).
        // By recording "processed" AFTER sending, a crash causes a DUPLICATE EMAIL upon redelivery.
        // In transactional e-commerce, a duplicate order receipt is strictly preferable to a missed receipt.
        const dummyEmailId = `email_${Date.now()}`;
        console.log(`Email Service: Confirmation email dispatched to user ${userId} for order: ${orderId} (Email ID: ${dummyEmailId})`);

        // 3. Atomically Record Idempotency & Queue Outbox Event for email-successful
        if (db) {
          const newEmailEventId = crypto.randomUUID();
          const rootCorrelationId = String(correlationId || eventId || newEmailEventId);
          const directCausationId = String(eventId || rootCorrelationId);

          try {
            await db.transaction(async (tx) => {
              if (eventId) {
                await tx.insert(processedEvents).values({
                  eventId: String(eventId),
                  serviceName: "email-service",
                });
              }

              await tx.insert(outboxEvents).values({
                id: newEmailEventId,
                topic: "email-successful",
                payload: {
                  eventId: newEmailEventId,
                  correlationId: rootCorrelationId,
                  causationId: directCausationId,
                  orderId,
                  userId,
                  emailId: dummyEmailId,
                },
                published: false,
              });
            });
            console.log(`Email Service: Outbox event queued for email-successful (Event ID: ${newEmailEventId})`);
          } catch (dbErr) {
            // Item 4: Unique constraint violation (code 23505) indicates concurrent duplicate.
            // Treat as "already processed, skip" without retrying and without sending to DLQ.
            if (
              dbErr.code === "23505" ||
              dbErr.message?.includes("duplicate key") ||
              dbErr.message?.includes("processed_events_pkey") ||
              dbErr.message?.includes("unique constraint")
            ) {
              console.warn(`Email Service [Idempotency]: Unique constraint violation for event ID ${eventId}. Concurrent duplicate detected; skipping cleanly.`);
              return;
            }

            console.error("Email Service: Failed to record processed_events / outbox in DB:", dbErr.message);
            throw dbErr;
          }
        }
      } finally {
        inflightMessages = Math.max(0, inflightMessages - 1);
      }
    },
  });
};

run().catch((err) => {
  console.error("Email Service Kafka error:", err);
});

// Graceful shutdown
const shutdown = async () => {
  if (isShuttingDown) return;
  isShuttingDown = true;
  isReady = false;
  console.log("Email Service: SIGTERM received. Initiating graceful shutdown...");

  healthServer.close();

  const maxWait = 5000;
  const start = Date.now();
  while (inflightMessages > 0 && Date.now() - start < maxWait) {
    await new Promise((r) => setTimeout(r, 200));
  }

  try {
    await stopEmailOutboxWorker();
    console.log("Email Service: Outbox worker stopped.");
    await consumer.disconnect();
    console.log("Email Service: Consumer disconnected.");
    await producer.disconnect();
    console.log("Email Service: Producer disconnected.");
  } catch (err) {
    console.error("Email Service error during disconnect:", err.message);
  }

  console.log("Email Service: Graceful shutdown complete. Exiting cleanly.");
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
