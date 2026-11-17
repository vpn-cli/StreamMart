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
  console.log(`🚀 Analytic service health server listening on port ${HEALTH_PORT}`);
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

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      inflightMessages++;
      try {
        const value = message.value.toString();
        const data = JSON.parse(value);

        switch (topic) {
          case "payment-successful": {
            const { userId, cart, eventId } = data;
            const total = cart && Array.isArray(cart)
              ? cart.reduce((acc, item) => acc + (Number(item.price) || 0), 0).toFixed(2)
              : "0.00";
            console.log(`Analytic consumer [payment-successful]: User ${userId} paid $${total} (Event: ${eventId || "n/a"})`);
            break;
          }
          case "order-successful": {
            const { userId, orderId, eventId } = data;
            console.log(`Analytic consumer [order-successful]: Order ${orderId} confirmed for user ${userId} (Event: ${eventId || "n/a"})`);
            break;
          }
          case "email-successful": {
            const { userId, emailId, orderId, eventId } = data;
            console.log(`Analytic consumer [email-successful]: Email ${emailId} dispatched to user ${userId} for order ${orderId}`);
            break;
          }
          default:
            console.log(`Analytic consumer [${topic}]: Received event`);
            break;
        }
      } finally {
        inflightMessages = Math.max(0, inflightMessages - 1);
      }
    },
  });
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
