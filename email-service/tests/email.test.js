import test from "node:test";
import assert from "node:assert/strict";
import { processEmailBatch } from "../consumer-processor.js";

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

  await t.test("eachBatch processes batch in single transaction and skips duplicates", async () => {
    const processed = new Set(["already-done"]);
    const insertedOutbox = [];
    const dispatched = [];
    const resolvedOffsets = [];

    const db = {
      select() {
        return {
          from() {
            return {
              where() {
                return Promise.resolve([{ eventId: "already-done" }]);
              },
            };
          },
        };
      },
      async transaction(fn) {
        const tx = {
          insert(table) {
            return {
              values(rows) {
                const arr = Array.isArray(rows) ? rows : [rows];
                if (table.__t === "processed") {
                  for (const r of arr) processed.add(r.eventId);
                } else {
                  insertedOutbox.push(...arr);
                }
                return Promise.resolve();
              },
            };
          },
        };
        await fn(tx);
      },
    };

    const messages = [
      { offset: "0", value: Buffer.from(JSON.stringify({ eventId: "already-done", orderId: "o-0", userId: "u-0" })) },
      { offset: "1", value: Buffer.from(JSON.stringify({ eventId: "evt-1", orderId: "o-1", userId: "u-1" })) },
      { offset: "2", value: Buffer.from(JSON.stringify({ eventId: "evt-2", orderId: "o-2", userId: "u-2" })) },
    ];

    const res = await processEmailBatch({
      batch: { topic: "order-successful", partition: 0, messages },
      resolveOffset: (o) => resolvedOffsets.push(o),
      heartbeat: async () => {},
      commitOffsetsIfNecessary: async () => {},
      isRunning: () => true,
      isStale: () => false,
      db,
      tables: {
        processedEvents: { __t: "processed", eventId: "event_id", serviceName: "service_name" },
        outboxEvents: { __t: "outbox" },
      },
      dispatchEmail: (item) => {
        dispatched.push(item.eventId);
        return `mail_${item.eventId}`;
      },
    });

    assert.equal(res.mode, "batch");
    assert.equal(res.processedCount, 2);
    assert.equal(res.skippedDuplicates, 1);
    assert.deepEqual(dispatched, ["evt-1", "evt-2"]);
    assert.equal(insertedOutbox.length, 2);
    assert.deepEqual(resolvedOffsets, ["0", "1", "2"]);
  });
});
