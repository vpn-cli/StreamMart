import http from "node:http";
import { Kafka } from "kafkajs";

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
  console.log(`🚀 Email service health server listening on port ${HEALTH_PORT}`);
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
        const { userId, orderId, eventId } = JSON.parse(value);

        const dummyEmailId = `email_${Date.now()}`;
        console.log(`Email consumer: Confirmation email dispatched to user ${userId} for order: ${orderId}`);

        await producer.send({
          topic: "email-successful",
          messages: [
            {
              key: String(orderId || userId),
              value: JSON.stringify({ userId, emailId: dummyEmailId, orderId, eventId }),
              headers: {
                "event-id": String(eventId || ""),
                "order-id": String(orderId || ""),
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
