import express from "express";
import cors from "cors";
import crypto from "node:crypto";
import { db } from "./db/index.js";
import { payments, outboxEvents } from "./db/schema.js";
import { startOutboxWorker, stopOutboxWorker, isRelayRunning } from "./worker.js";

const app = express();

app.use(cors());
app.use(express.json());

// Health Check for Docker Compose and Kubernetes probes
// Returns 503 Service Unavailable if the outbox relay loop is not actively running
app.get("/health", (req, res) => {
  if (!isRelayRunning()) {
    return res.status(503).send("Payment Service Relay Loop Not Running");
  }
  return res.status(200).send("Payment Service OK");
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
    startOutboxWorker();
    console.log(`🚀 Payment service is running on port ${PORT}`);
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
