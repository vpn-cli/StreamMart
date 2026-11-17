import { db } from "./db/index.js";
import { sql, inArray } from "drizzle-orm";
import { outboxEvents } from "./db/schema.js";
import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "email-service-worker",
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

let isRunning = false;
let loopPromise = null;
let isConnected = false;

/**
 * Process a single batch of unpublished email outbox events using
 * SELECT ... FOR UPDATE SKIP LOCKED to prevent duplicate publishes across workers.
 * Deterministic partition key: orderId (no fallback chains).
 */
export const processEmailOutboxBatch = async () => {
  if (!db || !isConnected) return 0;

  let processedCount = 0;

  try {
    await db.transaction(async (tx) => {
      const rows = await tx.execute(
        sql`SELECT id, topic, payload, created_at FROM outbox_events WHERE published = false AND topic = 'email-successful' ORDER BY created_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED`
      );

      const events = Array.from(rows || []);
      if (events.length === 0) {
        return;
      }

      const publishedIds = [];

      for (const event of events) {
        const rawPayload = typeof event.payload === "string" ? JSON.parse(event.payload) : event.payload;

        const eventId = String(rawPayload.eventId || event.id);
        const correlationId = String(rawPayload.correlationId || eventId);
        const causationId = String(rawPayload.causationId || eventId);
        const orderId = String(rawPayload.orderId);

        const enrichedPayload = {
          ...rawPayload,
          eventId,
          correlationId,
          causationId,
          orderId,
        };

        // Deterministic partition key: orderId (strictly 1 key, no fallback chains)
        const partitionKey = String(orderId);

        await producer.send({
          topic: event.topic,
          messages: [
            {
              key: partitionKey,
              value: JSON.stringify(enrichedPayload),
              headers: {
                "event-id": eventId,
                "correlation-id": correlationId,
                "causation-id": causationId,
                "order-id": orderId,
                "event-type": String(event.topic),
              },
            },
          ],
        });

        publishedIds.push(event.id);
        processedCount++;
      }

      if (publishedIds.length > 0) {
        await tx
          .update(outboxEvents)
          .set({ published: true })
          .where(inArray(outboxEvents.id, publishedIds));
        console.log(`Email Outbox Worker: Batch-published ${publishedIds.length} email-successful events`);
      }
    });
  } catch (err) {
    console.error("Email Outbox Worker: Batch processing failed (transaction rolled back):", err.message);
  }

  return processedCount;
};

const runLoop = async () => {
  while (isRunning) {
    try {
      const processedCount = await processEmailOutboxBatch();
      if (!isRunning) break;

      if (processedCount === 0) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    } catch (err) {
      console.error("Email Outbox Worker: Unexpected loop error:", err);
      if (isRunning) {
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }
};

export const startEmailOutboxWorker = async () => {
  if (!process.env.DATABASE_URL) return;
  if (isRunning) return;

  try {
    await producer.connect();
    isConnected = true;
    isRunning = true;
    console.log("Email Outbox Worker: Connected to Kafka. Starting relay loop.");
    loopPromise = runLoop();
  } catch (err) {
    console.error("Email Outbox Worker: Initial Kafka Connection Error", err.message);
  }
};

export const stopEmailOutboxWorker = async () => {
  console.log("Email Outbox Worker: Stopping worker loop...");
  isRunning = false;
  if (loopPromise) {
    await loopPromise;
    loopPromise = null;
  }
  if (isConnected) {
    try {
      await producer.disconnect();
      isConnected = false;
      console.log("Email Outbox Worker: Disconnected from Kafka");
    } catch (err) {
      console.error("Email Outbox Worker: Error disconnecting from Kafka", err.message);
    }
  }
};
