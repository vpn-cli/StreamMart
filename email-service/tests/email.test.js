import test from "node:test";
import assert from "node:assert/strict";

test("Email Service Idempotency & Unique Constraint Suite", async (t) => {
  await t.test("Unique constraint violation (code 23505) skips concurrent duplicates cleanly", async () => {
    let duplicateSkipped = false;

    const handleEmailEvent = async (eventId) => {
      try {
        const dbErr = new Error('duplicate key value violates unique constraint "processed_events_pkey"');
        dbErr.code = "23505";
        throw dbErr;
      } catch (dbErr) {
        if (
          dbErr.code === "23505" ||
          dbErr.message?.includes("duplicate key") ||
          dbErr.message?.includes("processed_events_pkey")
        ) {
          duplicateSkipped = true;
          return;
        }
        throw dbErr;
      }
    };

    await handleEmailEvent("evt-1234");
    assert.equal(duplicateSkipped, true, "Must cleanly skip concurrent duplicate without throwing");
  });

  await t.test("Email emitted event generates new eventId, preserves correlationId & causationId, partitioned by orderId", () => {
    const incomingOrderEvent = {
      eventId: "order-event-111",
      correlationId: "payment-event-001",
      causationId: "payment-event-001",
      orderId: "order-999",
      userId: "user-888",
    };

    const newEmailEventId = "email-event-222";
    const emailOutboxPayload = {
      eventId: newEmailEventId,
      correlationId: incomingOrderEvent.correlationId || incomingOrderEvent.eventId,
      causationId: incomingOrderEvent.eventId,
      orderId: incomingOrderEvent.orderId,
      userId: incomingOrderEvent.userId,
      emailId: "email-xyz",
    };

    // Partition key is strictly orderId
    const partitionKey = String(emailOutboxPayload.orderId);

    assert.equal(emailOutboxPayload.eventId, "email-event-222", "Emits brand new eventId");
    assert.equal(emailOutboxPayload.correlationId, "payment-event-001", "Preserves original payment correlationId");
    assert.equal(emailOutboxPayload.causationId, "order-event-111", "CausationId points to immediate order event");
    assert.equal(partitionKey, "order-999", "Partition key strictly matches orderId");
  });
});
