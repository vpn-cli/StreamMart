import http from "node:http";
import { Kafka } from "kafkajs";
import { db } from "./db/index.js";
import { orders, outboxEvents, processedEvents } from "./db/schema.js";
import { startOrderOutboxWorker, stopOrderOutboxWorker } from "./worker.js";
import { processOrderBatch, processSingleOrderMessage } from "./consumer-processor.js";

let isReady = false;
let isShuttingDown = false;
let inflightMessages = 0;

// High-resolution timing metrics buffer
const timingMetrics = {
  total: [],
  idempotency: [],
  tx: [],
  overhead: [],
};

export function getConsumerStats() {
  const calc = (arr) => {
    if (arr.length === 0) return { avg: 0, p50: 0, p95: 0, min: 0, max: 0 };
    const sorted = [...arr].sort((a, b) => a - b);
    const avg = arr.reduce((a, b) => a + b, 0) / arr.length;
    const p50 = sorted[Math.floor(sorted.length * 0.5)];
    const p95 = sorted[Math.floor(sorted.length * 0.95)];
    const min = sorted[0];
    const max = sorted[sorted.length - 1];
    return {
      avg: Number(avg.toFixed(2)),
      p50: Number(p50.toFixed(2)),
      p95: Number(p95.toFixed(2)),
      min: Number(min.toFixed(2)),
      max: Number(max.toFixed(2)),
    };
  };

  const total = calc(timingMetrics.total);
  const idempotency = calc(timingMetrics.idempotency);
  const tx = calc(timingMetrics.tx);
  const overhead = calc(timingMetrics.overhead);

  return {
    sampleCount: timingMetrics.total.length,
    total,
    idempotency,
    tx,
    overhead,
    impliedThroughput: total.avg > 0 ? Number((1000 / total.avg).toFixed(2)) : 0,
  };
}

// Health check server for container orchestration & metrics
const healthServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    if (isReady && !isShuttingDown) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("Order Service OK");
    } else {
      res.writeHead(503, { "Content-Type": "text/plain" });
      res.end(isShuttingDown ? "Order Service Shutting Down" : "Order Service Starting...");
    }
  } else if (req.url === "/consumer-stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(getConsumerStats(), null, 2));
  } else {
    res.writeHead(404);
    res.end();
  }
});

const HEALTH_PORT = process.env.PORT || 8001;
healthServer.listen(HEALTH_PORT, () => {
  const effectivePoolMax = parseInt(process.env.DB_POOL_MAX || "10", 10);
  const effectiveConsumerMode = process.env.CONSUMER_MODE || "eachMessage";
  const effectiveConcurrency = parseInt(process.env.PARTITIONS_CONSUMED_CONCURRENTLY || "1", 10);
  const effectiveRetentionHours = parseInt(process.env.OUTBOX_RETENTION_HOURS || "1", 10);
  console.log(`🚀 Order service health server listening on port ${HEALTH_PORT}`);
  console.log(
    `[EFFECTIVE CONFIG] order-service: PORT=${HEALTH_PORT}, DB_POOL_MAX=${effectivePoolMax}, CONSUMER_MODE=${effectiveConsumerMode}, PARTITIONS_CONSUMED_CONCURRENTLY=${effectiveConcurrency}, OUTBOX_RETENTION_HOURS=${effectiveRetentionHours}, KAFKA_BROKERS=${process.env.KAFKA_BROKERS || "localhost:9092"}`
  );
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
      await startOrderOutboxWorker();
      connected = true;
      isReady = true;
      console.log("Order Service: Connected to Kafka and subscribed to payment-successful");
    } catch (err) {
      console.warn("Order Service: Retrying Kafka connection/subscription in 2s...", err.message);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  if (isShuttingDown) return;

  const partitionsConsumedConcurrently = parseInt(process.env.PARTITIONS_CONSUMED_CONCURRENTLY || "1", 10);
  const consumerMode = process.env.CONSUMER_MODE || "eachMessage"; // "eachMessage" or "eachBatch"
  console.log(`Order Service: Running consumer with mode=${consumerMode}, partitionsConsumedConcurrently=${partitionsConsumedConcurrently}`);

  const tables = { orders, processedEvents, outboxEvents };

  if (consumerMode === "eachBatch") {
    await consumer.run({
      partitionsConsumedConcurrently,
      eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
        if (!isRunning() || isStale()) return;
        inflightMessages += batch.messages.length;

        try {
          const result = await processOrderBatch({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary,
            isRunning,
            isStale,
            db,
            producer,
            tables,
          });

          if (result.processedCount > 0) {
            const tTotal = result.tTotal || 0;
            const tIdemp = result.tIdemp || 0;
            const tTx = result.tTx || 0;
            const tOverhead = Math.max(0, tTotal - tIdemp - tTx);
            const perMsgTotal = tTotal / result.processedCount;
            const perMsgIdemp = tIdemp / result.processedCount;
            const perMsgTx = tTx / result.processedCount;
            const perMsgOverhead = tOverhead / result.processedCount;

            for (let i = 0; i < result.processedCount; i++) {
              timingMetrics.total.push(perMsgTotal);
              timingMetrics.idempotency.push(perMsgIdemp);
              timingMetrics.tx.push(perMsgTx);
              timingMetrics.overhead.push(perMsgOverhead);
            }

            if (timingMetrics.total.length % 200 < result.processedCount) {
              const stats = getConsumerStats();
              console.log(
                `📊 [Order eachBatch N=${stats.sampleCount} msgs] Total avg=${stats.total.avg}ms/msg | DB Tx avg=${stats.tx.avg}ms/msg | Implied Rate=${stats.impliedThroughput} msgs/s`
              );
            }
          }
        } finally {
          inflightMessages = Math.max(0, inflightMessages - batch.messages.length);
        }
      },
    });
  } else {
    // Sequential eachMessage processing
    await consumer.run({
      partitionsConsumedConcurrently,
      eachMessage: async ({ topic, partition, message }) => {
        inflightMessages++;
        try {
          const result = await processSingleOrderMessage({
            message,
            topic,
            partition,
            db,
            producer,
            tables,
          });

          if (result.status === "created") {
            const tTotal = result.tTotal || 0;
            const tIdemp = result.tIdemp || 0;
            const tTx = result.tTx || 0;
            const tOverhead = Math.max(0, tTotal - tIdemp - tTx);
            timingMetrics.total.push(tTotal);
            timingMetrics.idempotency.push(tIdemp);
            timingMetrics.tx.push(tTx);
            timingMetrics.overhead.push(tOverhead);

            if (timingMetrics.total.length % 100 === 0) {
              const stats = getConsumerStats();
              console.log(`📊 [Order Consumer Breakdown N=${stats.sampleCount}] Total=${stats.total.avg}ms | DB Tx=${stats.tx.avg}ms (${((stats.tx.avg / stats.total.avg) * 100).toFixed(1)}%) | Idempotency SELECT=${stats.idempotency.avg}ms (${((stats.idempotency.avg / stats.total.avg) * 100).toFixed(1)}%) | CPU/Overhead=${stats.overhead.avg}ms | Implied Rate=${stats.impliedThroughput} msgs/s`);
            }
          }
        } finally {
          inflightMessages = Math.max(0, inflightMessages - 1);
        }
      },
    });
  }
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
    await stopOrderOutboxWorker();
    console.log("Order Service: Outbox worker stopped.");
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
