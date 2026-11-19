import test from "node:test";
import assert from "node:assert/strict";
import { processOrderBatch, processSingleOrderMessage } from "../consumer-processor.js";

function createFakeDb() {
  const state = {
    orders: [],
    processedEvents: new Map(), // key: `${eventId}|${serviceName}`
    outboxEvents: [],
    orderSeq: 0,
  };

  const tables = {
    orders: { __table: "orders" },
    processedEvents: { __table: "processed_events", eventId: "event_id", serviceName: "service_name" },
    outboxEvents: { __table: "outbox_events" },
  };

  function makeTx(snapshot) {
    return {
      insert(table) {
        return {
          values(rows) {
            const arr = Array.isArray(rows) ? rows : [rows];
            if (table.__table === "orders") {
              const inserted = arr.map((r) => {
                if (r.userId === null || r.userId === undefined) {
                  const err = new Error('null value in column "user_id" of relation "orders" violates not-null constraint');
                  err.code = "23502";
                  throw err;
                }
                if (r.items === null || r.items === undefined) {
                  const err = new Error('null value in column "items" of relation "orders" violates not-null constraint');
                  err.code = "23502";
                  throw err;
                }
                const row = { id: `order-${++snapshot.orderSeq}`, ...r };
                snapshot.orders.push(row);
                return row;
              });
              return {
                returning: async () => inserted,
              };
            }
            if (table.__table === "processed_events") {
              for (const r of arr) {
                const key = `${r.eventId}|${r.serviceName}`;
                if (snapshot.processedEvents.has(key)) {
                  const err = new Error('duplicate key value violates unique constraint "processed_events_pkey"');
                  err.code = "23505";
                  throw err;
                }
                snapshot.processedEvents.set(key, r);
              }
              return Promise.resolve();
            }
            if (table.__table === "outbox_events") {
              for (const r of arr) {
                snapshot.outboxEvents.push(r);
              }
              return Promise.resolve();
            }
            return Promise.resolve();
          },
        };
      },
    };
  }

  function extractQueryValues(node, out = []) {
    if (!node || typeof node !== "object") return out;
    if (Array.isArray(node)) {
      for (const item of node) extractQueryValues(item, out);
      return out;
    }
    if (Object.prototype.hasOwnProperty.call(node, "value")) {
      const v = node.value;
      if (typeof v === "string") out.push(v);
      else if (Array.isArray(v)) {
        for (const item of v) {
          if (typeof item === "string") out.push(item);
          else extractQueryValues(item, out);
        }
      }
    }
    if (node.queryChunks) extractQueryValues(node.queryChunks, out);
    return out;
  }

  const db = {
    _state: state,
    select() {
      return {
        from() {
          return {
            where(condition) {
              const queriedStrings = new Set(extractQueryValues(condition));
              const rows = Array.from(state.processedEvents.values())
                .filter((r) => queriedStrings.has(r.eventId))
                .map((r) => ({
                  eventId: r.eventId,
                  serviceName: r.serviceName,
                }));
              const p = Promise.resolve(rows);
              p.limit = () => Promise.resolve(rows.slice(0, 1));
              return p;
            },
          };
        },
      };
    },
    async transaction(fn) {
      // Atomic snapshot rollback semantics
      const snapshot = {
        orders: [...state.orders],
        processedEvents: new Map(state.processedEvents),
        outboxEvents: [...state.outboxEvents],
        orderSeq: state.orderSeq,
      };
      const tx = makeTx(snapshot);
      await fn(tx);
      // Commit snapshot on success
      state.orders = snapshot.orders;
      state.processedEvents = snapshot.processedEvents;
      state.outboxEvents = snapshot.outboxEvents;
      state.orderSeq = snapshot.orderSeq;
    },
  };

  return { db, tables, state };
}

function makeMessage(offset, payload) {
  return {
    offset: String(offset),
    value: Buffer.from(typeof payload === "string" ? payload : JSON.stringify(payload)),
  };
}

test("Order Service Consumer Processor (eachBatch & eachMessage) Suite", async (t) => {
  await t.test("eachBatch: 1 poison DB message among 50 falls back 1-by-1 -> 49 orders, 1 DLQ, all 50 offsets resolved, consumer not stuck", async () => {
    const { db, tables, state } = createFakeDb();
    const dlqMessages = [];
    const producer = {
      async send(arg) {
        if (arg.topic === "payment-successful.DLQ") {
          dlqMessages.push(...arg.messages);
        }
      },
    };

    // Build 50 messages: 49 valid + 1 poison (userId: null triggers PG 23502 NOT NULL) at index 24
    const messages = [];
    for (let i = 0; i < 50; i++) {
      if (i === 24) {
        messages.push(
          makeMessage(i, {
            eventId: "poison-evt-24",
            userId: null,
            cart: [{ id: "bad", price: 10 }],
          })
        );
      } else {
        messages.push(
          makeMessage(i, {
            eventId: `evt-${i}`,
            userId: `user-${i}`,
            cart: [{ id: `item-${i}`, price: 25 }],
          })
        );
      }
    }

    const resolvedOffsets = [];
    let heartbeats = 0;
    let commits = 0;

    const result = await processOrderBatch({
      batch: { topic: "payment-successful", partition: 0, messages },
      resolveOffset: (o) => resolvedOffsets.push(o),
      heartbeat: async () => { heartbeats++; },
      commitOffsetsIfNecessary: async () => { commits++; },
      isRunning: () => true,
      isStale: () => false,
      db,
      producer,
      tables,
      hasDatabaseUrl: true,
      maxRetries: 3,
      retryBaseDelayMs: 1,
    });

    assert.equal(result.mode, "fallback_single", "Must fall back to per-message processing on batch tx failure");
    assert.equal(result.createdCount, 49, "Exactly 49 valid orders must be created");
    assert.equal(result.dlqCount, 1, "Exactly 1 poison message must be routed to DLQ");
    assert.equal(state.orders.length, 49, "DB orders table must contain 49 rows");
    assert.equal(state.outboxEvents.length, 49, "DB outbox_events table must contain 49 rows");
    assert.equal(dlqMessages.length, 1, "DLQ must receive exactly 1 message");
    assert.equal(dlqMessages[0].headers["original-event-id"], "poison-evt-24");
    assert.equal(dlqMessages[0].headers["error-reason"], "DB_INSERTION_EXHAUSTED");
    assert.equal(resolvedOffsets.length, 50, "All 50 offsets must be resolved");
    assert.deepEqual(
      resolvedOffsets,
      Array.from({ length: 50 }, (_, i) => String(i)),
      "Offsets must be resolved in strict ascending order"
    );
    assert.ok(commits >= 1, "Offsets must be committed");
    assert.ok(heartbeats >= 50, "Heartbeats must be called during fallback");

    // Verify consumer is NOT stuck: a subsequent healthy batch processes normally in batch mode
    const followUp = await processOrderBatch({
      batch: {
        topic: "payment-successful",
        partition: 0,
        messages: [makeMessage(50, { eventId: "evt-50", userId: "user-50", cart: [{ price: 15 }] })],
      },
      resolveOffset: (o) => resolvedOffsets.push(o),
      heartbeat: async () => {},
      commitOffsetsIfNecessary: async () => {},
      isRunning: () => true,
      isStale: () => false,
      db,
      producer,
      tables,
      hasDatabaseUrl: true,
    });
    assert.equal(followUp.mode, "batch");
    assert.equal(state.orders.length, 50);
  });

  await t.test("eachBatch: 1 malformed JSON among 50 creates 49 orders in 1 batch tx and sends 1 to DLQ", async () => {
    const { db, tables, state } = createFakeDb();
    const dlqMessages = [];
    const producer = {
      async send(arg) {
        if (arg.topic === "payment-successful.DLQ") dlqMessages.push(...arg.messages);
      },
    };

    const messages = [];
    for (let i = 0; i < 50; i++) {
      if (i === 10) {
        messages.push(makeMessage(i, "{not-valid-json"));
      } else {
        messages.push(makeMessage(i, { eventId: `json-evt-${i}`, userId: `u-${i}`, cart: [] }));
      }
    }

    const resolvedOffsets = [];
    const result = await processOrderBatch({
      batch: { topic: "payment-successful", partition: 0, messages },
      resolveOffset: (o) => resolvedOffsets.push(o),
      heartbeat: async () => {},
      commitOffsetsIfNecessary: async () => {},
      isRunning: () => true,
      isStale: () => false,
      db,
      producer,
      tables,
      hasDatabaseUrl: true,
    });

    assert.equal(result.mode, "batch");
    assert.equal(state.orders.length, 49);
    assert.equal(dlqMessages.length, 1);
    assert.equal(dlqMessages[0].headers["error-reason"], "JSON_PARSE_ERROR");
    assert.equal(resolvedOffsets.length, 50);
  });

  await t.test("eachBatch: crash after DB commit before offset commit -> redelivery produces 0 duplicates and all orders present", async () => {
    const { db, tables, state } = createFakeDb();
    const producer = { async send() {} };

    const messages = Array.from({ length: 20 }, (_, i) =>
      makeMessage(i, {
        eventId: `crash-evt-${i}`,
        userId: `crash-user-${i}`,
        cart: [{ price: 10 }],
        ...(i === 19 ? { testCrashAfterDbCommit: true } : {}),
      })
    );

    const resolvedBeforeCrash = [];
    let crashed = false;

    await assert.rejects(
      () =>
        processOrderBatch({
          batch: { topic: "payment-successful", partition: 0, messages },
          resolveOffset: (o) => resolvedBeforeCrash.push(o),
          heartbeat: async () => {},
          commitOffsetsIfNecessary: async () => {},
          isRunning: () => true,
          isStale: () => false,
          db,
          producer,
          tables,
          hasDatabaseUrl: true,
          onCrashAfterDbCommit: () => {
            crashed = true;
            throw new Error("SIMULATED_SIGKILL_AFTER_DB_COMMIT");
          },
        }),
      /SIMULATED_SIGKILL_AFTER_DB_COMMIT/
    );

    assert.equal(crashed, true);
    assert.equal(state.orders.length, 20, "All 20 orders committed to DB before crash");
    assert.equal(resolvedBeforeCrash.length, 0, "Zero offsets resolved before crash");

    // Simulate restart & Kafka redelivery of the exact same 20 messages
    const resolvedAfterRestart = [];
    let crashHookFiredAgain = false;
    const redeliveryResult = await processOrderBatch({
      batch: { topic: "payment-successful", partition: 0, messages },
      resolveOffset: (o) => resolvedAfterRestart.push(o),
      heartbeat: async () => {},
      commitOffsetsIfNecessary: async () => {},
      isRunning: () => true,
      isStale: () => false,
      db,
      producer,
      tables,
      hasDatabaseUrl: true,
      onCrashAfterDbCommit: () => {
        crashHookFiredAgain = true;
      },
    });

    assert.equal(crashHookFiredAgain, false, "Crash hook must not re-fire when redelivered batch has 0 new items");
    assert.equal(redeliveryResult.createdCount, 0, "Zero duplicate orders created on redelivery");
    assert.equal(redeliveryResult.skippedDuplicates, 20, "All 20 redelivered messages skipped via processed_events");
    assert.equal(state.orders.length, 20, "Total orders remains exactly 20 (0 duplicates)");
    assert.equal(resolvedAfterRestart.length, 20, "All 20 offsets resolved after restart");
  });

  await t.test("eachMessage: unique constraint violation (23505) skips concurrent duplicate cleanly without retries or DLQ", async () => {
    const dlqMessages = [];
    let attempts = 0;
    const { tables } = createFakeDb();

    const db = {
      select() {
        return {
          from() {
            return {
              where() {
                return { limit: async () => [] };
              },
            };
          },
        };
      },
      async transaction() {
        attempts++;
        const err = new Error('duplicate key value violates unique constraint "processed_events_pkey"');
        err.code = "23505";
        throw err;
      },
    };

    const producer = {
      async send(arg) {
        dlqMessages.push(...arg.messages);
      },
    };

    const result = await processSingleOrderMessage({
      message: makeMessage(0, { eventId: "dup-evt-1", userId: "u-1", cart: [] }),
      db,
      producer,
      tables,
      hasDatabaseUrl: true,
      maxRetries: 3,
      retryBaseDelayMs: 1,
    });

    assert.equal(result.status, "skipped_concurrent_duplicate");
    assert.equal(attempts, 1, "Must not retry on 23505");
    assert.equal(dlqMessages.length, 0, "Must not route concurrent duplicate to DLQ");
  });
});
