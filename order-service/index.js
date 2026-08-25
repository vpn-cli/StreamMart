import { Kafka } from "kafkajs";
import { supabase } from "./supabaseClient.js";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "order-service" });

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, cart } = JSON.parse(value);

        // Create order on real DB (Supabase)
        let orderId = "mock-order-id";
        if (process.env.SUPABASE_URL !== 'placeholder') {
          const { data, error } = await supabase
            .from("orders")
            .insert([{ user_id: userId, items: cart, status: "processing" }])
            .select()
            .single();
            
          if (error) {
            console.error("Database error inserting order. Make sure 'orders' table exists: ", error.message);
          } else if (data) {
            orderId = data.id;
          }
        }
        
        console.log(`Order consumer: Order processed for user id: ${userId}, orderId: ${orderId}`);

        await producer.send({
          topic: "order-successful",
          messages: [
            { value: JSON.stringify({ userId, orderId }) },
          ],
        });
      },
    });
  } catch (err) {
    console.log(err);
  }
};

run();
