import http from "node:http";
import { Kafka } from "kafkajs";
import { db } from "./db/index.js";
import { orders } from "./db/schema.js";

let isReady = false;

// Health check server for container orchestration
const healthServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    if (isReady) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Order Service OK");
    } else {
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end("Order Service Starting...");
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
      mechanism: 'plain',
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    },
  }),
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "order-service" });

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true,
    });

    isReady = true;
    console.log("Order Service: Connected to Kafka and subscribed to payment-successful");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, cart, eventId } = JSON.parse(value);

        // Create order on real DB (Drizzle ORM)
        let orderId = "mock-order-id";
        if (process.env.DATABASE_URL) {
          try {
            const data = await db.insert(orders).values({
              userId: userId,
              items: cart,
              status: "processing"
            }).returning();
            
            if (data && data.length > 0) {
              orderId = data[0].id;
            }
          } catch (e) {
            console.error("Database error inserting order. Make sure 'orders' table exists: ", e.message);
          }
        }
        
        console.log(`Order consumer: Order processed for user id: ${userId}, orderId: ${orderId}`);

        await producer.send({
          topic: "order-successful",
          messages: [
            { key: orderId !== "mock-order-id" ? orderId : userId, value: JSON.stringify({ userId, orderId, eventId }) },
          ],
        });
      },
    });
  } catch (err) {
    console.error("Order Service Kafka error:", err);
  }
};

run();

// Graceful shutdown
const shutdown = async () => {
  console.log("Order Service: shutting down gracefully...");
  isReady = false;
  healthServer.close();
  try {
    await consumer.disconnect();
    await producer.disconnect();
  } catch (err) {
    console.error("Order Service error during disconnect:", err);
  }
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
