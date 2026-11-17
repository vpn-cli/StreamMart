import { db } from "./db/index.js";
import { outboxEvents } from "./db/schema.js";
import { eq } from "drizzle-orm";
import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-service-worker",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
  ...(process.env.KAFKA_SASL_USERNAME && {
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    },
  }),
});

const producer = kafka.producer();

let pollInterval = null;

export const stopOutboxWorker = async () => {
  if (pollInterval) {
    clearInterval(pollInterval);
    pollInterval = null;
  }
  try {
    await producer.disconnect();
    console.log("Outbox Worker: Disconnected from Kafka");
  } catch (err) {
    console.error("Outbox Worker: Error disconnecting from Kafka", err);
  }
};

export const startOutboxWorker = async () => {
  if (!process.env.DATABASE_URL) return;

  try {
    await producer.connect();
    console.log("Outbox Worker: Connected to Kafka");

    // Poll every 3 seconds for unpublished events
    pollInterval = setInterval(async () => {
      const events = await db
        .select()
        .from(outboxEvents)
        .where(eq(outboxEvents.published, false))
        .limit(50); // Batch limit

      if (!events || events.length === 0) return;

      for (const event of events) {
        try {
          // Attempt to publish
          await producer.send({
            topic: event.topic,
            messages: [{ value: JSON.stringify(event.payload) }],
          });

          // Committing the publish back to DB only if Kafka strictly succeeds
          await db
            .update(outboxEvents)
            .set({ published: true })
            .where(eq(outboxEvents.id, event.id));
            
          console.log(`Outbox Worker: Confirmed publish for event ID: ${event.id}`);
        } catch (err) {
          console.error(`Outbox Worker: Failed to publish event ID ${event.id} (Leaving as unpublished to retry again)`, err);
        }
      }
    }, 5000); 
  } catch (err) {
    console.error("Outbox Worker: Initial Kafka Connection Error", err);
  }
};
