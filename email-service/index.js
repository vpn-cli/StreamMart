import http from "node:http";
import { Kafka } from "kafkajs";
import { db } from "./db/index.js";
import { outboxEvents, processedEvents } from "./db/schema.js";
import { startEmailOutboxWorker, stopEmailOutboxWorker } from "./worker.js";
import { processEmailBatch, processSingleEmailMessage } from "./consumer-processor.js";

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
  const effectivePoolMax = parseInt(process.env.DB_POOL_MAX || "10", 10);
  const effectiveConsumerMode = process.env.EMAIL_CONSUMER_MODE || process.env.CONSUMER_MODE || "eachBatch";
  const effectiveRetentionHours = parseInt(process.env.OUTBOX_RETENTION_HOURS || "1", 10);
  console.log(`🚀 Email service health server listening on port ${HEALTH_PORT}`);
  console.log(
    `[EFFECTIVE CONFIG] email-service: PORT=${HEALTH_PORT}, DB_POOL_MAX=${effectivePoolMax}, CONSUMER_MODE=${effectiveConsumerMode}, OUTBOX_RETENTION_HOURS=${effectiveRetentionHours}, KAFKA_BROKERS=${process.env.KAFKA_BROKERS || "localhost:9092"}`
  );
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
      await startEmailOutboxWorker();
      connected = true;
      isReady = true;
      console.log("Email Service: Connected to Kafka and subscribed to order-successful");
    } catch (err) {
      console.warn("Email Service: Retrying Kafka connection/subscription in 2s...", err.message);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  if (isShuttingDown) return;

  const consumerMode = process.env.EMAIL_CONSUMER_MODE || process.env.CONSUMER_MODE || "eachBatch";
  const tables = { processedEvents, outboxEvents };
  console.log(`Email Service: Running consumer with mode=${consumerMode}`);

  if (consumerMode === "eachBatch") {
    await consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
        if (!isRunning() || isStale()) return;
        inflightMessages += batch.messages.length;
        try {
          await processEmailBatch({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary,
            isRunning,
            isStale,
            db,
            tables,
          });
        } finally {
          inflightMessages = Math.max(0, inflightMessages - batch.messages.length);
        }
      },
    });
  } else {
    await consumer.run({
      eachMessage: async ({ message }) => {
        inflightMessages++;
        try {
          await processSingleEmailMessage({
            message,
            db,
            tables,
          });
        } finally {
          inflightMessages = Math.max(0, inflightMessages - 1);
        }
      },
    });
  }
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
    await stopEmailOutboxWorker();
    console.log("Email Service: Outbox worker stopped.");
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
