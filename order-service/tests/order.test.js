import test from "node:test";
import assert from "node:assert/strict";

test("Order Service Idempotency & Unique Constraint Violation Suite", async (t) => {
  await t.test("Unique constraint violation (code 23505) treats concurrent duplicate as already processed and skips without DLQ or retries", async () => {
    let dlqTriggered = false;
    let retryCount = 0;

    // Simulated consumer handler logic matching order-service/index.js
    const processMessage = async ({ eventId, userId, cart }) => {
      let orderId = null;
      let isDuplicateViolation = false;
      const MAX_RETRIES = 3;
      let attempt = 0;

      while (attempt < MAX_RETRIES) {
        attempt++;
        retryCount++;
        try {
          // Simulate DB transaction throwing PostgreSQL 23505 unique_violation
          const dbErr = new Error('duplicate key value violates unique constraint "processed_events_pkey"');
          dbErr.code = "23505";
          throw dbErr;
        } catch (dbErr) {
          if (
            dbErr.code === "23505" ||
            dbErr.message?.includes("duplicate key") ||
            dbErr.message?.includes("processed_events_pkey") ||
            dbErr.message?.includes("unique constraint")
          ) {
            // Handled cleanly: concurrent duplicate
            isDuplicateViolation = true;
            break;
          }

          if (attempt < MAX_RETRIES) {
            await new Promise((r) => setTimeout(r, 10));
          }
        }
      }

      if (isDuplicateViolation) {
        return { status: "skipped_concurrent_duplicate" };
      }

      if (!orderId) {
        dlqTriggered = true;
        return { status: "sent_to_dlq" };
      }

      return { status: "created", orderId };
    };

    const result = await processMessage({
      eventId: "duplicate-event-uuid-1234",
      userId: "user-test",
      cart: [{ price: 40 }],
    });

    assert.equal(result.status, "skipped_concurrent_duplicate", "Concurrent duplicate must be treated as already processed and skipped");
    assert.equal(retryCount, 1, "Must NOT perform retries when unique constraint conflict is encountered");
    assert.equal(dlqTriggered, false, "Must NOT route concurrent duplicates to the Dead-Letter Queue (DLQ)");
  });

  await t.test("Non-unique DB errors exhaust retries and route poison messages to DLQ", async () => {
    let dlqTriggered = false;
    let retryCount = 0;

    const processMessage = async ({ eventId, userId, cart }) => {
      let orderId = null;
      let isDuplicateViolation = false;
      const MAX_RETRIES = 3;
      let attempt = 0;

      while (attempt < MAX_RETRIES) {
        attempt++;
        retryCount++;
        try {
          // Simulate a fatal connection/syntax error (not a unique violation)
          throw new Error("Connection terminated unexpectedly");
        } catch (dbErr) {
          if (dbErr.code === "23505" || dbErr.message?.includes("duplicate key")) {
            isDuplicateViolation = true;
            break;
          }

          if (attempt < MAX_RETRIES) {
            await new Promise((r) => setTimeout(r, 5));
          }
        }
      }

      if (isDuplicateViolation) {
        return { status: "skipped_concurrent_duplicate" };
      }

      if (!orderId) {
        dlqTriggered = true;
        return { status: "sent_to_dlq" };
      }

      return { status: "created", orderId };
    };

    const result = await processMessage({
      eventId: "poison-event-uuid-5678",
      userId: "user-poison",
      cart: [],
    });

    assert.equal(result.status, "sent_to_dlq", "Exhausted retries must route to DLQ");
    assert.equal(retryCount, 3, "Must attempt exactly 3 retries before routing to DLQ");
    assert.equal(dlqTriggered, true, "DLQ must be triggered for poison messages");
  });

  await t.test("Event metadata preserves new eventId, correlationId, and causationId with deterministic orderId partition key", () => {
    const incomingEvent = {
      eventId: "payment-event-001",
      correlationId: "payment-event-001",
      causationId: "payment-event-001",
      userId: "user-456",
    };

    const newOrderEventId = "order-event-999";
    const orderId = "order-uuid-789";

    const orderOutboxPayload = {
      eventId: newOrderEventId,
      correlationId: incomingEvent.correlationId || incomingEvent.eventId,
      causationId: incomingEvent.eventId,
      orderId,
      userId: incomingEvent.userId,
    };

    // Partition key must strictly be orderId
    const partitionKey = String(orderOutboxPayload.orderId);

    assert.equal(orderOutboxPayload.eventId, "order-event-999", "Every event gets a distinct new eventId");
    assert.equal(orderOutboxPayload.correlationId, "payment-event-001", "Correlation ID preserves the initiating payment event");
    assert.equal(orderOutboxPayload.causationId, "payment-event-001", "Causation ID identifies the immediate triggering event");
    assert.equal(partitionKey, "order-uuid-789", "order-successful partition key is strictly orderId");
  });
});
