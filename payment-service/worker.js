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

const OUTBOX_RETENTION_HOURS = parseInt(process.env.OUTBOX_RETENTION_HOURS || "1", 10);
let lastPruneTime = 0;
const PRUNE_INTERVAL_MS = 60 * 1000;

export const prunePublishedEvents = async (force = false) => {
  if (!db) return 0;
  const now = Date.now();
  if (!force && now - lastPruneTime < PRUNE_INTERVAL_MS) return 0;
  lastPruneTime = now;

  try {
    const cutoffDate = new Date(now - OUTBOX_RETENTION_HOURS * 60 * 60 * 1000);
    const result = await db.execute(
      sql`DELETE FROM outbox_events WHERE published = true AND topic = 'payment-successful' AND created_at < ${cutoffDate}`
    );
    const deletedCount = result?.rowCount || 0;
    if (deletedCount > 0) {
      console.log(`Outbox Pruner: Deleted ${deletedCount} published outbox events older than ${OUTBOX_RETENTION_HOURS} hour(s).`);
    }
    return deletedCount;
  } catch (err) {
    console.error("Outbox Pruner: Prune operation failed:", err.message);
    return 0;
  }
};

let shouldRun = false;
let isRunning = false;
let isConnected = false;
let loopPromise = null;
let retryTimer = null;

export const isRelayRunning = () => isRunning && isConnected;

if (producer.on && producer.events) {
  producer.on(producer.events.CONNECT, () => {
    console.log("Outbox Worker: Kafka producer connected event received.");
    isConnected = true;
  });

  producer.on(producer.events.DISCONNECT, () => {
    console.warn("Outbox Worker: Kafka producer disconnected event received.");
    isConnected = false;
    isRunning = false;
  });
}

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
      // Strictly filter to topic = 'payment-successful' for the payment service relay
      const rows = await tx.execute(
        sql`SELECT id, topic, payload, created_at FROM outbox_events WHERE published = false AND topic = 'payment-successful' ORDER BY created_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED`
      );

      const events = Array.from(rows || []);
      if (events.length === 0) {
        return;
      }

      const publishedIds = [];
      const messagesByTopic = new Map();

      for (const event of events) {
        const rawPayload = typeof event.payload === "string" ? JSON.parse(event.payload) : event.payload;

        const eventId = String(rawPayload.eventId || event.id);
        const correlationId = String(rawPayload.correlationId || eventId);
        const causationId = String(rawPayload.causationId || eventId);

        const enrichedPayload = {
          ...rawPayload,
          eventId,
          correlationId,
          causationId,
        };

        // Deterministic partition key: userId (no fallback chains)
        const partitionKey = String(enrichedPayload.userId);

        if (!messagesByTopic.has(event.topic)) {
          messagesByTopic.set(event.topic, []);
        }

        messagesByTopic.get(event.topic).push({
          key: partitionKey,
          value: JSON.stringify(enrichedPayload),
          headers: {
            "event-id": eventId,
            "correlation-id": correlationId,
            "causation-id": causationId,
            "event-type": String(event.topic),
          },
        });

        publishedIds.push(event.id);
        processedCount++;
      }

      if (process.env.RELAY_BATCH_SEND === "true") {
        // Batch publish messages per topic in single network request
        for (const [topic, messages] of messagesByTopic.entries()) {
          await producer.send({
            topic,
            messages,
          });
        }
      } else {
        // Sequential publish (original baseline behavior)
        for (const [topic, messages] of messagesByTopic.entries()) {
          for (const msg of messages) {
            await producer.send({
              topic,
              messages: [msg],
            });
          }
        }
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
 * Handles initial Kafka connection with backoff, batch processing, and auto-reconnect.
 */
const runLoop = async () => {
  let backoffDelay = 1000;
  const maxDelay = 10000;

  while (shouldRun) {
    // 1. Ensure Kafka connection with retry and exponential backoff
    if (!isConnected) {
      isRunning = false;
      try {
        console.log(`Outbox Worker: Connecting to Kafka broker(s) (${process.env.KAFKA_BROKERS || "localhost:9092"})...`);
        await producer.connect();
        isConnected = true;
        isRunning = true;
        backoffDelay = 1000;
        console.log("Outbox Worker: Connected to Kafka. Relay loop active.");
      } catch (err) {
        isConnected = false;
        isRunning = false;
        console.error(`Outbox Worker: Kafka connect failed: ${err.message}. Retrying in ${backoffDelay}ms...`);
        if (!shouldRun) break;
        await new Promise((resolve) => {
          retryTimer = setTimeout(resolve, backoffDelay);
        });
        retryTimer = null;
        backoffDelay = Math.min(backoffDelay * 2, maxDelay);
        continue;
      }
    }

    // 2. Process outbox batch
    try {
      const processedCount = await processOutboxBatch();
      if (!shouldRun) break;

      // 3. Periodically prune published outbox events older than OUTBOX_RETENTION_HOURS
      await prunePublishedEvents();

      // If events were processed, apply busy pause; otherwise wait 500ms
      const busyPause = parseInt(process.env.RELAY_BUSY_PAUSE_MS || "25", 10);
      const delay = processedCount === 0 ? 500 : busyPause;
      await new Promise((resolve) => {
        retryTimer = setTimeout(resolve, delay);
      });
      retryTimer = null;
    } catch (err) {
      console.error("Outbox Worker: Unexpected loop error:", err.message || err);
      if (
        err.name === "KafkaJSConnectionError" ||
        err.name === "KafkaJSNumberOfRetriesExceeded" ||
        (err.message && (err.message.includes("connection") || err.message.includes("disconnected")))
      ) {
        isConnected = false;
        isRunning = false;
      }
      if (shouldRun) {
        await new Promise((resolve) => {
          retryTimer = setTimeout(resolve, 1000);
        });
        retryTimer = null;
      }
    }
  }

  isRunning = false;
};

export const startOutboxWorker = () => {
  if (!process.env.DATABASE_URL) return;
  if (shouldRun) return loopPromise;

  shouldRun = true;
  loopPromise = runLoop().catch((err) => {
    console.error("Outbox Worker: Fatal error in relay loop:", err);
    isRunning = false;
    isConnected = false;
  });
  return loopPromise;
};

export const stopOutboxWorker = async () => {
  console.log("Outbox Worker: Stopping worker loop...");
  shouldRun = false;
  isRunning = false;

  if (retryTimer) {
    clearTimeout(retryTimer);
    retryTimer = null;
  }

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
