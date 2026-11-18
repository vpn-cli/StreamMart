import crypto from "node:crypto";
import { performance } from "node:perf_hooks";
import { and, eq, inArray } from "drizzle-orm";

export function isDuplicateConstraintError(err) {
  const code = err?.code || err?.cause?.code;
  const msg = err?.message || "";
  const causeMsg = err?.cause?.message || "";
  return (
    code === "23505" ||
    msg.includes("duplicate key") ||
    msg.includes("processed_events_pkey") ||
    msg.includes("unique constraint") ||
    causeMsg.includes("duplicate key") ||
    causeMsg.includes("processed_events_pkey") ||
    causeMsg.includes("unique constraint")
  );
}

export async function sendToDlq({
  producer,
  originalPayload,
  rawValue,
  eventId,
  userId,
  errorReason,
  errorMessage,
  partition,
  offset,
}) {
  await producer.send({
    topic: "payment-successful.DLQ",
    messages: [
      {
        key: String(eventId || userId || "unknown"),
        value: JSON.stringify({
          originalPayload: originalPayload ?? null,
          ...(rawValue !== undefined ? { rawValue } : {}),
          error: errorMessage,
          failedAt: new Date().toISOString(),
          eventId: eventId || null,
          ...(partition !== undefined ? { partition } : {}),
          ...(offset !== undefined ? { offset } : {}),
        }),
        headers: {
          "error-reason": String(errorReason),
          "original-event-id": String(eventId || ""),
        },
      },
    ],
  });
}

export async function processSingleOrderMessage({
  message,
  topic = "payment-successful",
  partition = 0,
  db,
  producer,
  tables,
  hasDatabaseUrl = Boolean(process.env.DATABASE_URL),
  maxRetries = 3,
  retryBaseDelayMs = 100,
  onCrashAfterDbCommit = () => {
    console.warn("💥 [TEST HOOK]: Simulating immediate order-service crash after DB commit (before relay publish)!");
    process.exit(99);
  },
}) {
  const { orders, processedEvents, outboxEvents } = tables;
  const t0 = performance.now();
  let tIdemp = 0;
  let tTx = 0;

  const rawValue = message.value ? message.value.toString() : "";
  let data;
  try {
    data = JSON.parse(rawValue);
  } catch (parseErr) {
    console.error(`Order Service [POISON MESSAGE][JSON Error] offset=${message.offset}:`, parseErr.message);
    await sendToDlq({
      producer,
      originalPayload: null,
      rawValue,
      eventId: null,
      userId: null,
      errorReason: "JSON_PARSE_ERROR",
      errorMessage: `Invalid JSON: ${parseErr.message}`,
      partition,
      offset: message.offset,
    });
    return {
      status: "sent_to_dlq",
      reason: "JSON_PARSE_ERROR",
      tTotal: performance.now() - t0,
      tIdemp,
      tTx,
    };
  }

  const userId = data.userId == null ? null : String(data.userId);
  const cart = data.cart === undefined ? [] : data.cart;
  const eventId = data.eventId == null ? null : String(data.eventId);
  const { correlationId, causationId } = data;

  // 1. Idempotency Check: if eventId has already been processed by order-service, skip side-effect
  if (hasDatabaseUrl && db && eventId) {
    const tIdemp0 = performance.now();
    try {
      const existing = await db
        .select()
        .from(processedEvents)
        .where(
          and(
            eq(processedEvents.eventId, eventId),
            eq(processedEvents.serviceName, "order-service")
          )
        )
        .limit(1);

      tIdemp = performance.now() - tIdemp0;
      if (existing && existing.length > 0) {
        console.warn(`Order Service [Idempotency]: Event ID ${eventId} already processed by order-service. Skipping duplicate order creation.`);
        return {
          status: "skipped_duplicate",
          tTotal: performance.now() - t0,
          tIdemp,
          tTx,
        };
      }
    } catch (e) {
      tIdemp = performance.now() - tIdemp0;
      console.error("Order Service: Error querying processed_events:", e.message);
    }
  }

  // 2. Atomic Order Creation + Outbox Event + Idempotency Record in ONE SQL transaction
  let orderId = null;
  let isDuplicateViolation = false;
  let lastErrorMessage = `Exhausted ${maxRetries} DB insert retries`;
  let attempt = 0;

  if (hasDatabaseUrl && db) {
    while (attempt < maxRetries) {
      attempt++;
      try {
        const orderEventId = crypto.randomUUID();
        const rootCorrelationId = String(correlationId || eventId || orderEventId);
        const directCausationId = String(eventId || rootCorrelationId);

        const tTx0 = performance.now();
        await db.transaction(async (tx) => {
          const inserted = await tx
            .insert(orders)
            .values({
              userId,
              items: cart,
              status: "processing",
            })
            .returning();

          if (inserted && inserted.length > 0) {
            orderId = inserted[0].id;
          }

          if (eventId) {
            await tx.insert(processedEvents).values({
              eventId,
              serviceName: "order-service",
            });
          }

          await tx.insert(outboxEvents).values({
            id: orderEventId,
            topic: "order-successful",
            payload: {
              eventId: orderEventId,
              correlationId: rootCorrelationId,
              causationId: directCausationId,
              orderId,
              userId,
              items: cart,
            },
            published: false,
          });
        });
        tTx = performance.now() - tTx0;
        break;
      } catch (dbErr) {
        if (isDuplicateConstraintError(dbErr)) {
          console.warn(`Order Service [Idempotency]: Unique constraint violation for event ID ${eventId}. Concurrent duplicate detected; skipping cleanly.`);
          isDuplicateViolation = true;
          break;
        }

        lastErrorMessage = dbErr?.cause?.message || dbErr?.message || String(dbErr);
        console.error(`Order Service: DB write failed on attempt ${attempt}/${maxRetries}:`, lastErrorMessage);
        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, retryBaseDelayMs * Math.pow(2, attempt - 1)));
        }
      }
    }
  } else {
    orderId = `order_${Date.now()}`;
  }

  if (isDuplicateViolation) {
    return {
      status: "skipped_concurrent_duplicate",
      tTotal: performance.now() - t0,
      tIdemp,
      tTx,
    };
  }

  if (!orderId) {
    console.error(`Order Service [POISON MESSAGE]: Failed to create order after ${maxRetries} attempts. Routing to payment-successful.DLQ`);
    await sendToDlq({
      producer,
      originalPayload: data,
      eventId,
      userId,
      errorReason: "DB_INSERTION_EXHAUSTED",
      errorMessage: lastErrorMessage,
      partition,
      offset: message.offset,
    });
    return {
      status: "sent_to_dlq",
      reason: "DB_INSERTION_EXHAUSTED",
      tTotal: performance.now() - t0,
      tIdemp,
      tTx,
    };
  }

  if (process.env.TEST_CRASH_AFTER_DB_COMMIT === "true" || data.testCrashAfterDbCommit === true) {
    onCrashAfterDbCommit();
  }

  return {
    status: "created",
    orderId,
    tTotal: performance.now() - t0,
    tIdemp,
    tTx,
  };
}

export async function processOrderBatch({
  batch,
  resolveOffset,
  heartbeat,
  commitOffsetsIfNecessary,
  isRunning,
  isStale,
  db,
  producer,
  tables,
  hasDatabaseUrl = Boolean(process.env.DATABASE_URL),
  maxRetries = 3,
  retryBaseDelayMs = 100,
  onCrashAfterDbCommit = () => {
    console.warn("💥 [TEST HOOK]: Simulating immediate order-service crash after DB commit (before relay publish / offset commit)!");
    process.exit(99);
  },
  onHoldBeforeOffsetCommit = async () => {
    console.warn("⏸️ [TEST HOOK]: DB batch committed; holding before offset commit (waiting for SIGKILL)...");
    await new Promise(() => {});
  },
}) {
  if (!isRunning() || isStale()) {
    return { mode: "skipped_not_running", createdCount: 0, dlqCount: 0, skippedDuplicates: 0 };
  }

  const { orders, processedEvents, outboxEvents } = tables;
  const t0 = performance.now();
  let tIdemp = 0;
  let tTx = 0;

  const parsedItems = [];
  const parseFailedItems = [];

  for (const message of batch.messages) {
    const raw = message.value ? message.value.toString() : "";
    try {
      const data = JSON.parse(raw);
      parsedItems.push({
        message,
        data,
        eventId: data.eventId == null ? "" : String(data.eventId),
        userId: data.userId == null ? null : String(data.userId),
        cart: data.cart === undefined ? [] : data.cart,
        correlationId: data.correlationId,
        causationId: data.causationId,
      });
    } catch (parseErr) {
      parseFailedItems.push({ message, raw, parseErr });
    }
  }

  // 1. Bulk idempotency check with IN query
  const eventIds = parsedItems.map((p) => p.eventId).filter(Boolean);
  let existingSet = new Set();
  if (hasDatabaseUrl && db && eventIds.length > 0) {
    const tIdemp0 = performance.now();
    try {
      const existing = await db
        .select({ eventId: processedEvents.eventId })
        .from(processedEvents)
        .where(
          and(
            inArray(processedEvents.eventId, eventIds),
            eq(processedEvents.serviceName, "order-service")
          )
        );
      existingSet = new Set(existing.map((e) => e.eventId));
    } catch (e) {
      console.error("Order Service: Bulk idempotency check failed:", e.message);
    }
    tIdemp = performance.now() - tIdemp0;
  }

  // Filter out already-processed and within-batch duplicate eventIds
  const seenInBatch = new Set();
  const newItems = [];
  let skippedDuplicates = 0;
  for (const item of parsedItems) {
    if (item.eventId && (existingSet.has(item.eventId) || seenInBatch.has(item.eventId))) {
      skippedDuplicates++;
      continue;
    }
    if (item.eventId) seenInBatch.add(item.eventId);
    newItems.push(item);
  }

  if (skippedDuplicates > 0) {
    console.warn(
      `Order Service [eachBatch Idempotency]: Skipped ${skippedDuplicates} already-processed duplicate message(s) in batch of ${batch.messages.length}.`
    );
  }

  // 2. Single atomic SQL transaction for all new items in the batch
  let batchFailed = false;
  let batchError = null;

  if (newItems.length > 0 && hasDatabaseUrl && db) {
    const tTx0 = performance.now();
    try {
      await db.transaction(async (tx) => {
        const insertedOrders = await tx
          .insert(orders)
          .values(
            newItems.map((item) => ({
              userId: item.userId,
              items: item.cart,
              status: "processing",
            }))
          )
          .returning();

        const processedValues = newItems
          .filter((item) => item.eventId)
          .map((item) => ({
            eventId: item.eventId,
            serviceName: "order-service",
          }));

        if (processedValues.length > 0) {
          await tx.insert(processedEvents).values(processedValues);
        }

        await tx.insert(outboxEvents).values(
          newItems.map((item, idx) => {
            const orderId = insertedOrders[idx]?.id;
            const orderEventId = crypto.randomUUID();
            const rootCorrelationId = String(item.correlationId || item.eventId || orderEventId);
            const directCausationId = String(item.eventId || rootCorrelationId);
            return {
              id: orderEventId,
              topic: "order-successful",
              payload: {
                eventId: orderEventId,
                correlationId: rootCorrelationId,
                causationId: directCausationId,
                orderId,
                userId: item.userId,
                items: item.cart,
              },
              published: false,
            };
          })
        );
      });
      tTx = performance.now() - tTx0;
    } catch (err) {
      tTx = performance.now() - tTx0;
      batchFailed = true;
      batchError = err;
    }
  }

  // 3. Fallback: if batch transaction failed, roll back happened cleanly -> process 1-by-1 in offset order
  if (batchFailed) {
    const reason = batchError?.cause?.message || batchError?.message || String(batchError);
    console.warn(
      `Order Service [eachBatch Fallback]: Batch transaction failed (${reason}). Falling back to per-message processing for ${batch.messages.length} message(s)...`
    );

    let fallbackCreated = 0;
    let fallbackDlq = 0;
    let fallbackSkipped = 0;

    for (const message of batch.messages) {
      if (!isRunning() || isStale()) break;

      const res = await processSingleOrderMessage({
        message,
        topic: batch.topic,
        partition: batch.partition,
        db,
        producer,
        tables,
        hasDatabaseUrl,
        maxRetries,
        retryBaseDelayMs,
        onCrashAfterDbCommit,
      });

      if (res.status === "created") fallbackCreated++;
      else if (res.status === "sent_to_dlq") fallbackDlq++;
      else fallbackSkipped++;

      resolveOffset(message.offset);
      await heartbeat();
    }

    if (commitOffsetsIfNecessary) {
      await commitOffsetsIfNecessary();
    }

    return {
      mode: "fallback_single",
      createdCount: fallbackCreated,
      dlqCount: fallbackDlq,
      skippedDuplicates: fallbackSkipped,
      tTotal: performance.now() - t0,
      tIdemp,
      tTx,
      processedCount: batch.messages.length,
    };
  }

  // 4. Test hooks: fire AFTER DB batch commit and BEFORE offsets are resolved/committed
  // Only trigger when newItems were actually committed in this batch (so redelivery skips cleanly)
  if (newItems.length > 0) {
    if (
      process.env.TEST_HOLD_BEFORE_OFFSET_COMMIT === "true" ||
      newItems.some((item) => item.data?.testHoldBeforeOffsetCommit === true)
    ) {
      await onHoldBeforeOffsetCommit();
    } else if (
      process.env.TEST_CRASH_AFTER_DB_COMMIT === "true" ||
      newItems.some((item) => item.data?.testCrashAfterDbCommit === true)
    ) {
      onCrashAfterDbCommit();
    }
  }

  // 5. Route any JSON-parse-failed messages to DLQ before resolving batch offsets
  for (const failed of parseFailedItems) {
    console.error(`Order Service [POISON MESSAGE][JSON Error] offset=${failed.message.offset}:`, failed.parseErr.message);
    await sendToDlq({
      producer,
      originalPayload: null,
      rawValue: failed.raw,
      eventId: null,
      userId: null,
      errorReason: "JSON_PARSE_ERROR",
      errorMessage: `Invalid JSON: ${failed.parseErr.message}`,
      partition: batch.partition,
      offset: failed.message.offset,
    });
  }

  // 6. Resolve offsets in strict order and commit
  for (const message of batch.messages) {
    resolveOffset(message.offset);
  }
  if (commitOffsetsIfNecessary) {
    await commitOffsetsIfNecessary();
  }
  await heartbeat();

  return {
    mode: "batch",
    createdCount: newItems.length,
    dlqCount: parseFailedItems.length,
    skippedDuplicates,
    tTotal: performance.now() - t0,
    tIdemp,
    tTx,
    processedCount: batch.messages.length,
  };
}
