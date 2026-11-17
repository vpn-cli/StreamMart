import http from "node:http";
import { Kafka } from "kafkajs";

let isReady = false;

// Health check server for container orchestration
const healthServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    if (isReady) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Email Service OK");
    } else {
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end("Email Service Starting...");
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
      mechanism: 'plain',
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    },
  }),
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "email-service" });

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topic: "order-successful",
      fromBeginning: true,
    });

    isReady = true;
    console.log("Email Service: Connected to Kafka and subscribed to order-successful");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, orderId, eventId } = JSON.parse(value);

        const dummyEmailId = `email_${Date.now()}`;
        console.log(`Email consumer: Confirmation email sent to user id: ${userId} for order: ${orderId}`);

        await producer.send({
          topic: "email-successful",
          messages: [
            { key: orderId ? String(orderId) : String(userId), value: JSON.stringify({ userId, emailId: dummyEmailId, orderId, eventId }) },
          ],
        });
      },
    });
  } catch (err) {
    console.error("Email Service Kafka error:", err);
  }
};

run();

// Graceful shutdown
const shutdown = async () => {
  console.log("Email Service: shutting down gracefully...");
  isReady = false;
  healthServer.close();
  try {
    await consumer.disconnect();
    await producer.disconnect();
  } catch (err) {
    console.error("Email Service error during disconnect:", err);
  }
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
