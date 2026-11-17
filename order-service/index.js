import http from "node:http";
import { Kafka } from "kafkajs";
import { db } from "./db/index.js";
import { orders, processedEvents } from "./db/schema.js";
import { and, eq } from "drizzle-orm";

let isReady = false;
let isShuttingDown = false;
let inflightMessages = 0;

// Health check server for container orchestration
const healthServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    if (isReady && !isShuttingDown) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Order Service OK");
    } else {
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end(isShuttingDown ? "Order Service Shutting Down" : "Order Service Starting...");
    }
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
      connected = true;
      isReady = true;
      console.log("Order Service: Connected to Kafka and subscribed to payment-successful");
    } catch (err) {
      console.warn("Order Service: Retrying Kafka connection/subscription in 2s...", err.message);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  if (isShuttingDown) return;

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      inflightMessages++;
      try {
        const rawValue = message.value.toString();
        const data = JSON.parse(rawValue);
        const { userId, cart, eventId } = data;

        // 1. Idempotency Check: if eventId has already been processed by order-service, skip side-effect
        if (process.env.DATABASE_URL && eventId) {
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

            if (existing && existing.length > 0) {
              console.warn(`Order Service [Idempotency]: Event ID ${eventId} already processed by order-service. Skipping duplicate order creation.`);
              return;
            }
          } catch (e) {
            console.error("Order Service: Error querying processed_events:", e.message);
          }
        }

        // 2. Atomic Order Creation + Idempotency Record with Exponential Backoff Retry
        let orderId = null;
        const MAX_RETRIES = 3;
        let attempt = 0;

        if (process.env.DATABASE_URL) {
          while (attempt < MAX_RETRIES) {
            attempt++;
            try {
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
              });

              // Succeeded
              break;
            } catch (dbErr) {
              console.error(`Order Service: DB write failed on attempt ${attempt}/${MAX_RETRIES}:`, dbErr.message);
              if (attempt < MAX_RETRIES) {
                // Exponential backoff: 100ms, 200ms, 400ms
                await new Promise((resolve) => setTimeout(resolve, 100 * Math.pow(2, attempt - 1)));
              }
            }
          }
        } else {
          // In-memory or demo mode without DATABASE_URL
          orderId = `order_${Date.now()}`;
        }

        // 3. Poison Message Handling (DLQ): If all retries fail, do NOT emit ghost order
        if (!orderId) {
          console.error(`Order Service [POISON MESSAGE]: Failed to create order after ${MAX_RETRIES} attempts. Routing to payment-successful.DLQ`);
          await producer.send({
            topic: "payment-successful.DLQ",
            messages: [
              {
                key: String(userId || "unknown"),
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

        console.log(`Order Service: Order created successfully (ID: ${orderId}) for user: ${userId}`);

        // 4. Publish order-successful with partition key set to orderId
        await producer.send({
          topic: "order-successful",
          messages: [
            {
              key: String(orderId),
              value: JSON.stringify({ userId, orderId, eventId }),
              headers: {
                "event-id": String(eventId || ""),
                "order-id": String(orderId),
              },
            },
          ],
        });
      } finally {
        inflightMessages = Math.max(0, inflightMessages - 1);
      }
    },
  });
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
