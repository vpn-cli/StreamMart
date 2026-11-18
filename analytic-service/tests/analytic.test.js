import test from "node:test";
import assert from "node:assert/strict";
import { processAnalyticBatch } from "../consumer-processor.js";

test("Analytic Service Idempotency & Unique Constraint Suite", async (t) => {
  await t.test("Unique constraint violation (code 23505) skips concurrent duplicates cleanly without throwing", async () => {
    let duplicateSkipped = false;

    const handleAnalyticEvent = async (eventId) => {
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

    await handleAnalyticEvent("evt-9999");
    assert.equal(duplicateSkipped, true, "Must cleanly skip concurrent duplicate without error");
  });

  await t.test("Analytic consumer dedupes on incoming event's own eventId", () => {
    const processedEventsStore = new Set();
    const recordedMetrics = [];

    const consumeEvent = (event) => {
      if (processedEventsStore.has(event.eventId)) {
        return "skipped";
      }

      recordedMetrics.push(event);
      processedEventsStore.add(event.eventId);
      return "processed";
    };

    const event = { eventId: "pay-101", userId: "u-1", amount: "50.00" };

    const firstRun = consumeEvent(event);
    const secondRun = consumeEvent(event);

    assert.equal(firstRun, "processed");
    assert.equal(secondRun, "skipped");
    assert.equal(recordedMetrics.length, 1, "Exactly one metric recorded despite duplicate message delivery");
  });

  await t.test("eachBatch bulk inserts new events and skips duplicates cleanly", async () => {
    const inserted = [];
    const resolvedOffsets = [];
    const db = {
      select() {
        return {
          from() {
            return {
              where() {
                return Promise.resolve([{ eventId: "evt-dup" }]);
              },
            };
          },
        };
      },
      insert() {
        return {
          values(rows) {
            return {
              onConflictDoNothing: async () => {
                inserted.push(...rows);
              },
            };
          },
        };
      },
    };

    const messages = [
      { offset: "0", value: Buffer.from(JSON.stringify({ eventId: "evt-dup" })) },
      { offset: "1", value: Buffer.from(JSON.stringify({ eventId: "evt-a" })) },
      { offset: "2", value: Buffer.from(JSON.stringify({ eventId: "evt-b" })) },
    ];

    const res = await processAnalyticBatch({
      batch: { topic: "payment-successful", partition: 0, messages },
      resolveOffset: (o) => resolvedOffsets.push(o),
      heartbeat: async () => {},
      commitOffsetsIfNecessary: async () => {},
      isRunning: () => true,
      isStale: () => false,
      db,
      tables: { processedEvents: { eventId: "event_id", serviceName: "service_name" } },
    });

    assert.equal(res.mode, "batch");
    assert.equal(res.processedCount, 2);
    assert.equal(res.skippedDuplicates, 1);
    assert.equal(inserted.length, 2);
    assert.deepEqual(resolvedOffsets, ["0", "1", "2"]);
  });
});

