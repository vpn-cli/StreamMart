import request from "supertest";
import { jest } from "@jest/globals";

// Mock the Kafka producer to bypass connections
const mockConnect = jest.fn().mockResolvedValue(undefined);
const mockDisconnect = jest.fn().mockResolvedValue(undefined);
const mockSend = jest.fn().mockResolvedValue(undefined);
const mockOn = jest.fn();

jest.unstable_mockModule("kafkajs", () => {
  return {
    Kafka: class Kafka {
      producer() {
        return {
          connect: mockConnect,
          disconnect: mockDisconnect,
          send: mockSend,
          on: mockOn,
          events: {
            CONNECT: "producer.connect",
            DISCONNECT: "producer.disconnect",
          },
        };
      }
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
      }),
      execute: jest.fn().mockResolvedValue({ rowCount: 5 }),
    }
  };
});

const { default: app } = await import("../index.js");
const { prunePublishedEvents } = await import("../worker.js");

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

  it("GET /health returns 503 when the relay loop is not running", async () => {
    const res = await request(app).get("/health");
    expect(res.statusCode).toEqual(503);
    expect(res.text).toBe("Payment Service Relay Loop Not Running");
  });

  it("prunePublishedEvents deletes published events older than retention cutoff", async () => {
    const deleted = await prunePublishedEvents(true);
    expect(deleted).toBe(5);
  });
});
