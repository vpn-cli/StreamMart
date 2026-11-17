import http from "node:http";
import { Kafka } from "kafkajs";

let isReady = false;

// Health check server for container orchestration
const healthServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    if (isReady) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Analytic Service OK");
    } else {
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end("Analytic Service Starting...");
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
      mechanism: 'plain',
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    },
  }),
});

const consumer = kafka.consumer({ groupId: "analytic-service" });

const run = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({
      topics: ["payment-successful", "order-successful", "email-successful"],
      fromBeginning: true,
    });

    isReady = true;
    console.log("Analytic Service: Connected to Kafka and subscribed to topics");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const data = JSON.parse(value);

        switch (topic) {
          case "payment-successful": {
            const { userId, cart } = data;
            const total = cart && Array.isArray(cart)
              ? cart.reduce((acc, item) => acc + (Number(item.price) || 0), 0).toFixed(2)
              : "0.00";
            console.log(`Analytic consumer: User ${userId} paid $${total}`);
            break;
          }
          case "order-successful": {
            const { userId, orderId } = data;
            console.log(`Analytic consumer: Order id ${orderId} created for user id ${userId}`);
            break;
          }
          case "email-successful": {
            const { userId, emailId, orderId } = data;
            console.log(`Analytic consumer: Email id ${emailId} dispatched for user ${userId} (Order ${orderId})`);
            break;
          }
          default:
            break;
        }
      },
    });
  } catch (err) {
    console.error("Analytic Service Kafka error:", err);
  }
};

run();

// Graceful shutdown
const shutdown = async () => {
  console.log("Analytic Service: shutting down gracefully...");
  isReady = false;
  healthServer.close();
  try {
    await consumer.disconnect();
  } catch (err) {
    console.error("Analytic Service error during disconnect:", err);
  }
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
