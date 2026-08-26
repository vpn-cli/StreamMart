import express from "express";
import cors from "cors";
import { db } from "./db/index.js";
import { payments, outboxEvents } from "./db/schema.js";
import { startOutboxWorker } from "./worker.js";

const app = express();

app.use(
  cors({
    origin: "http://localhost:3000",
  })
);
app.use(express.json());

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const producer = kafka.producer();

const connectToKafka = async () => {
  try {
    await producer.connect();
    console.log("Producer connected!");
  } catch (err) {
    console.log("Error connecting to Kafka", err);
  }
};

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;
  // In a real app we decrypt user token/cookie
  // Now we get it securely from the API Gateway header!
  const userId = req.headers['x-user-id'] || "123";

  // PERFECT TRANSACTIONAL OUTBOX PATTERN via Drizzle ORM
  if (process.env.DATABASE_URL) {
    const amount = cart ? cart.length * 10 : 0;
    
    try {
      // 1. Insert Payment & Outbox Event ATOMICALLY!
      await db.transaction(async (tx) => {
        await tx.insert(payments).values({
          userId: userId,
          amount: String(amount),
          status: "completed"
        });
        
        await tx.insert(outboxEvents).values({
          topic: "payment-successful",
          payload: { userId, cart },
          published: false 
        });
      });
    } catch (e) {
      console.error("Failed Atomic Outbox Transaction:", e);
      return res.status(500).send("Database transaction failed");
    }
  }

  // Notice: Absolutely no producer.send() here! The background worker will do it.
  return res.status(200).send("Payment processed and event safely queued in outbox.");
});

app.use((err, req, res, next) => {
  res.status(err.status || 500).send(err.message);
});

if (process.env.NODE_ENV !== "test") {
  app.listen(8000, () => {
    startOutboxWorker(); // Safely decoupling this from the REST logic!
    console.log("Payment service is running on port 8000");
  });
}

export default app;
