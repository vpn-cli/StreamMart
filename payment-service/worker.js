import { db } from "./db/index.js";
import { sql, inArray } from "drizzle-orm";
import { outboxEvents } from "./db/schema.js";
import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-service-worker",
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
 * Process a single batch of unpublished outbox events using
 * SELECT ... FOR UPDATE SKIP LOCKED to prevent duplicate publishes across workers.
 * Updates published status in batch using ANY(...) array syntax.
 */
const processOutboxBatch = async () => {
  if (!db || !isConnected) return 0;

  let processedCount = 0;

  try {
    await db.transaction(async (tx) => {
      // 1. Concurrency-safe lock: claim up to 50 rows, skipping any locked by another worker
      const rows = await tx.execute(
        sql`SELECT id, topic, payload, created_at FROM outbox_events WHERE published = false ORDER BY created_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED`
      );

      const events = Array.from(rows || []);
      if (events.length === 0) {
        return;
      }

      const publishedIds = [];

      for (const event of events) {
        const rawPayload = typeof event.payload === "string" ? JSON.parse(event.payload) : event.payload;

        // Guarantee eventId is included in the message for downstream idempotency
        const enrichedPayload = {
          ...rawPayload,
          eventId: event.id,
        };

        // Partition by orderId or userId to guarantee strict in-order delivery per entity
        const partitionKey = String(enrichedPayload.orderId || enrichedPayload.userId || event.id);

        await producer.send({
          topic: event.topic,
          messages: [
            {
              key: partitionKey,
              value: JSON.stringify(enrichedPayload),
              headers: {
                "event-id": String(event.id),
                "event-type": String(event.topic),
              },
            },
          ],
        });

        publishedIds.push(event.id);
        processedCount++;
      }

      // Batch update all published events in a single SQL operation
      if (publishedIds.length > 0) {
        await tx
          .update(outboxEvents)
          .set({ published: true })
          .where(inArray(outboxEvents.id, publishedIds));
        console.log(`Outbox Worker: Batch-published ${publishedIds.length} events (IDs: ${publishedIds.slice(0, 3).join(", ")}${publishedIds.length > 3 ? "..." : ""})`);
      }
    });
  } catch (err) {
    console.error("Outbox Worker: Batch processing failed (transaction rolled back):", err.message);
  }

  return processedCount;
};

/**
 * Main async outbox relay loop:
 * Awaits batch completion before scheduling next iteration to prevent overlapping runs.
 */
const runLoop = async () => {
  while (isRunning) {
    try {
      const processedCount = await processOutboxBatch();
      if (!isRunning) break;

      // If events were processed, immediately check for more; otherwise wait 1s
      if (processedCount === 0) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    } catch (err) {
      console.error("Outbox Worker: Unexpected loop error:", err);
      if (isRunning) {
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }
};

export const startOutboxWorker = async () => {
  if (!process.env.DATABASE_URL) return;
  if (isRunning) return;

  try {
    await producer.connect();
    isConnected = true;
    isRunning = true;
    console.log("Outbox Worker: Connected to Kafka. Starting relay loop.");
    loopPromise = runLoop();
  } catch (err) {
    console.error("Outbox Worker: Initial Kafka Connection Error", err.message);
  }
};

export const stopOutboxWorker = async () => {
  console.log("Outbox Worker: Stopping worker loop...");
  isRunning = false;
  if (loopPromise) {
    await loopPromise;
    loopPromise = null;
  }
  if (isConnected) {
    try {
      await producer.disconnect();
      isConnected = false;
      console.log("Outbox Worker: Disconnected from Kafka");
    } catch (err) {
      console.error("Outbox Worker: Error disconnecting from Kafka", err.message);
    }
  }
};
