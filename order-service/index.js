import { Kafka } from "kafkajs";
import { db } from "./db/index.js";
import { orders } from "./db/schema.js";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
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

        // Create order on real DB (Drizzle ORM)
        let orderId = "mock-order-id";
        if (process.env.DATABASE_URL) {
          try {
            const data = await db.insert(orders).values({
              userId: userId,
              items: cart,
              status: "processing"
            }).returning();
            
            if (data && data.length > 0) {
              orderId = data[0].id;
            }
          } catch (e) {
            console.error("Database error inserting order. Make sure 'orders' table exists: ", e.message);
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
