import request from "supertest";
import { jest } from "@jest/globals";

// Mock the Kafka producer to bypass connections
jest.unstable_mockModule("kafkajs", () => {
  return {
    Kafka: class Kafka {
      producer() { return { connect: jest.fn(), send: jest.fn() }; }
      consumer() { return { connect: jest.fn(), subscribe: jest.fn(), run: jest.fn() }; }
    }
  };
});

// Mock Supabase to avoid hitting real DBs during unit tests
jest.unstable_mockModule("../supabaseClient.js", () => {
  return {
    supabase: {
      from: jest.fn().mockReturnThis(),
      insert: jest.fn().mockReturnValue({ error: null })
    }
  };
});

const { default: app } = await import("../index.js");

describe("POST /payment-service", () => {
  it("should output 200 upon successful payment processing and mock Kafka broadcast", async () => {
    const res = await request(app)
      .post("/payment-service")
      .send({ cart: [{ price: 50 }, { price: 20 }] });
      
    expect(res.statusCode).toEqual(200);
    expect(res.text).toBe("Payment processed and event broadcasted");
  });
});
