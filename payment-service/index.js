import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";
import { supabase } from "./supabaseClient.js";

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
  const userId = "123";

  // PAYMENT LOGIC via Supabase DB Persistence
  if (process.env.SUPABASE_URL !== 'placeholder') {
    const { error } = await supabase
      .from("payments")
      .insert([{ user_id: userId, amount: cart?.length * 10 || 0, status: "completed" }]);
      
    if (error) {
      console.error("Failed to insert payment record. Make sure 'payments' table exists:", error.message);
    }
  }

  // KAFKA NOTIFICATION
  try {
    await producer.send({
      topic: "payment-successful",
      messages: [{ value: JSON.stringify({ userId, cart }) }],
    });
  } catch (err) {
    console.error("Kafka error:", err);
    return res.status(500).send("Message broker failure");
  }

  return res.status(200).send("Payment processed and event broadcasted");
});

app.use((err, req, res, next) => {
  res.status(err.status || 500).send(err.message);
});

if (process.env.NODE_ENV !== "test") {
  app.listen(8000, () => {
    connectToKafka();
    console.log("Payment service is running on port 8000");
  });
}

export default app;
