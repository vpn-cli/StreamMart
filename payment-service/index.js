import express from "express";
import cors from "cors";
import crypto from "node:crypto";
import { db } from "./db/index.js";
import { payments, outboxEvents } from "./db/schema.js";
import { startOutboxWorker, stopOutboxWorker, isRelayRunning, getRelayStatus } from "./worker.js";

const app = express();

app.use(cors());
app.use(express.json());

// Liveness Check: returns 200 while the payment-service process is alive and responsive
app.get("/health", (req, res) => {
  return res.status(200).send("Payment Service OK");
});

// Relay Status Check: returns 200 if relay loop is active, 503 if disconnected from Kafka.
// Always includes outbox pruner health and last error for visibility.
app.get("/relay-status", (req, res) => {
  const status = getRelayStatus();
  if (!status.relayRunning) {
    return res.status(503).json({
      status: "unhealthy",
      message: "Payment Service Relay Loop Not Running",
      ...status,
    });
  }
  const hasPrunerError = Boolean(status.pruner?.lastError);
  return res.status(200).json({
    status: hasPrunerError ? "degraded" : "ok",
    message: hasPrunerError
      ? `Payment Service Relay Running (Pruner Error: ${status.pruner.lastError})`
      : "Payment Service Relay Running",
    ...status,
  });
});

const handlePayment = async (req, res) => {
  const { cart, testCrashAfterDbCommit } = req.body || {};
  // The authenticated user ID is verified and injected by the API Gateway
  const userId = req.headers["x-user-id"] || "123";

  if (process.env.DATABASE_URL) {
    const amount = cart && Array.isArray(cart) ? cart.reduce((sum, item) => sum + (Number(item.price) || 10), 0) : 0;
    
    try {
      // Generate unique eventId for the payment-successful event.
      // As the root initiating event, correlationId and causationId match eventId.
      const eventId = crypto.randomUUID();

      // Insert Payment & Outbox Event ATOMICALLY in a single SQL transaction
      await db.transaction(async (tx) => {
        await tx.insert(payments).values({
          userId: userId,
          amount: String(amount),
          status: "completed",
        });
        
        await tx.insert(outboxEvents).values({
          id: eventId,
          topic: "payment-successful",
          payload: {
            eventId,
            correlationId: eventId,
            causationId: eventId,
            userId,
            cart: cart || [],
            amount: String(amount),
            ...(testCrashAfterDbCommit && { testCrashAfterDbCommit: true }),
          },
          published: false,
        });
      });
    } catch (e) {
      console.error("Failed Atomic Outbox Transaction:", e);
      return res.status(500).json({ error: "Database transaction failed", details: e.message });
    }
  }

  return res.status(200).send("Payment processed and event safely queued in outbox.");
};

// Single authoritative payment route; aliases removed
app.post("/payment", handlePayment);

app.use((err, req, res, next) => {
  console.error("Payment Service Error:", err);
  res.status(err.status || 500).send(err.message);
});

let server;
if (process.env.NODE_ENV !== "test") {
  const PORT = process.env.PORT || 8000;
  server = app.listen(PORT, () => {
    const effectivePoolMax = parseInt(process.env.DB_POOL_MAX || "10", 10);
    const effectiveBatchSend = process.env.RELAY_BATCH_SEND === "true";
    const effectiveBusyPauseMs = parseInt(process.env.RELAY_BUSY_PAUSE_MS || "25", 10);
    const effectiveRetentionHours = parseInt(process.env.OUTBOX_RETENTION_HOURS || "1", 10);
    console.log(`🚀 Payment service is running on port ${PORT}`);
    console.log(
      `[EFFECTIVE CONFIG] payment-service: PORT=${PORT}, DB_POOL_MAX=${effectivePoolMax}, RELAY_BATCH_SEND=${effectiveBatchSend}, RELAY_BUSY_PAUSE_MS=${effectiveBusyPauseMs}, OUTBOX_RETENTION_HOURS=${effectiveRetentionHours}, KAFKA_BROKERS=${process.env.KAFKA_BROKERS || "localhost:9092"}`
    );
    startOutboxWorker();
  });
}

// Graceful shutdown
const shutdown = async () => {
  console.log("Payment Service: SIGTERM/SIGINT received, draining connections...");
  await stopOutboxWorker();
  if (server) {
    server.close(() => {
      console.log("Payment Service: HTTP server closed.");
      process.exit(0);
    });
  } else {
    process.exit(0);
  }
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

export default app;
