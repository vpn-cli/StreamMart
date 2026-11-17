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

// Mock Drizzle to avoid hitting real DBs during unit tests
jest.unstable_mockModule("../db/index.js", () => {
  return {
    db: {
      transaction: jest.fn(async (cb) => {
        const mockTx = {
          insert: jest.fn().mockReturnThis(),
          values: jest.fn()
        };
        return cb(mockTx);
      })
    }
  };
});

const { default: app } = await import("../index.js");

describe("Payment Service Routing & Endpoints", () => {
  it("POST /payment succeeds with 200 upon valid payment processing", async () => {
    const res = await request(app)
      .post("/payment")
      .send({ cart: [{ price: 50 }, { price: 20 }] });
      
    expect(res.statusCode).toEqual(200);
    expect(res.text).toBe("Payment processed and event safely queued in outbox.");
  });

  it("POST /payment-service is removed and returns 404", async () => {
    const res = await request(app)
      .post("/payment-service")
      .send({ cart: [] });
    expect(res.statusCode).toEqual(404);
  });

  it("POST / is removed and returns 404", async () => {
    const res = await request(app)
      .post("/")
      .send({ cart: [] });
    expect(res.statusCode).toEqual(404);
  });
});
