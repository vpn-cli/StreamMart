import crypto from "node:crypto";
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

export async function processSingleEmailMessage({
  message,
  db,
  tables,
  maxRetries = 3,
  retryBaseDelayMs = 100,
  dispatchEmail = ({ userId, orderId }) => {
    const dummyEmailId = `email_${Date.now()}_${crypto.randomUUID().slice(0, 8)}`;
    return dummyEmailId;
  },
}) {
  const { processedEvents, outboxEvents } = tables;
  const raw = message.value ? message.value.toString() : "";
  let data;
  try {
    data = JSON.parse(raw);
  } catch (parseErr) {
    console.error(`Email Service [JSON Error] offset=${message.offset}:`, parseErr.message);
    return { status: "skipped_invalid_json" };
  }

  const { userId, orderId, correlationId } = data;
  const eventId = data.eventId == null ? null : String(data.eventId);

  // 1. Idempotency check
  if (db && eventId) {
    try {
      const existing = await db
        .select({ eventId: processedEvents.eventId })
        .from(processedEvents)
        .where(
          and(
            eq(processedEvents.eventId, eventId),
            eq(processedEvents.serviceName, "email-service")
          )
        )
        .limit(1);

      if (existing && existing.length > 0) {
        return { status: "skipped_duplicate" };
      }
    } catch (e) {
      console.error("Email Service: Error querying processed_events:", e.message);
    }
  }

  // 2. External side-effect BEFORE DB commit (at-least-once email trade-off)
  const dummyEmailId = dispatchEmail({ userId, orderId, eventId });

  // 3. Atomically record processed_events + email-successful outbox row
  if (db) {
    const newEmailEventId = crypto.randomUUID();
    const rootCorrelationId = String(correlationId || eventId || newEmailEventId);
    const directCausationId = String(eventId || rootCorrelationId);

    let attempt = 0;
    while (attempt < maxRetries) {
      attempt++;
      try {
        await db.transaction(async (tx) => {
          if (eventId) {
            await tx.insert(processedEvents).values({
              eventId,
              serviceName: "email-service",
            });
          }

          await tx.insert(outboxEvents).values({
            id: newEmailEventId,
            topic: "email-successful",
            payload: {
              eventId: newEmailEventId,
              correlationId: rootCorrelationId,
              causationId: directCausationId,
              orderId,
              userId,
              emailId: dummyEmailId,
            },
            published: false,
          });
        });
        return { status: "processed", emailId: dummyEmailId };
      } catch (dbErr) {
        if (isDuplicateConstraintError(dbErr)) {
          console.warn(`Email Service [Idempotency]: Concurrent duplicate for event ${eventId}; skipping cleanly.`);
          return { status: "skipped_concurrent_duplicate" };
        }
        console.error(`Email Service: DB write failed on attempt ${attempt}/${maxRetries}:`, dbErr.message);
        if (attempt < maxRetries) {
          await new Promise((r) => setTimeout(r, retryBaseDelayMs * Math.pow(2, attempt - 1)));
        } else {
          return { status: "failed", error: dbErr.message };
        }
      }
    }
  }

  return { status: "processed", emailId: dummyEmailId };
}

export async function processEmailBatch({
  batch,
  resolveOffset,
  heartbeat,
  commitOffsetsIfNecessary,
  isRunning,
  isStale,
  db,
  tables,
  maxRetries = 3,
  retryBaseDelayMs = 100,
  dispatchEmail = ({ userId, orderId }) => `email_${Date.now()}_${crypto.randomUUID().slice(0, 8)}`,
}) {
  if (!isRunning() || isStale()) {
    return { mode: "skipped_not_running", processedCount: 0, skippedDuplicates: 0 };
  }

  const { processedEvents, outboxEvents } = tables;
  const parsedItems = [];

  for (const message of batch.messages) {
    const raw = message.value ? message.value.toString() : "";
    try {
      const data = JSON.parse(raw);
      parsedItems.push({
        message,
        data,
        eventId: data.eventId == null ? "" : String(data.eventId),
        userId: data.userId,
        orderId: data.orderId,
        correlationId: data.correlationId,
      });
    } catch (parseErr) {
      console.error(`Email Service [JSON Error] offset=${message.offset}:`, parseErr.message);
    }
  }

  // 1. Bulk idempotency check
  const eventIds = parsedItems.map((p) => p.eventId).filter(Boolean);
  let existingSet = new Set();
  if (db && eventIds.length > 0) {
    try {
      const existing = await db
        .select({ eventId: processedEvents.eventId })
        .from(processedEvents)
        .where(
          and(
            inArray(processedEvents.eventId, eventIds),
            eq(processedEvents.serviceName, "email-service")
          )
        );
      existingSet = new Set(existing.map((e) => e.eventId));
    } catch (e) {
      console.error("Email Service: Bulk idempotency check failed:", e.message);
    }
  }

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

  // 2. Dispatch external emails BEFORE recording processed_events (preserving e-commerce trade-off)
  const dispatched = newItems.map((item) => ({
    ...item,
    emailId: dispatchEmail({ userId: item.userId, orderId: item.orderId, eventId: item.eventId }),
  }));

  // 3. Single atomic SQL transaction for the entire batch
  let batchFailed = false;
  let batchErr = null;

  if (dispatched.length > 0 && db) {
    try {
      await db.transaction(async (tx) => {
        const processedValues = dispatched
          .filter((item) => item.eventId)
          .map((item) => ({
            eventId: item.eventId,
            serviceName: "email-service",
          }));

        if (processedValues.length > 0) {
          await tx.insert(processedEvents).values(processedValues);
        }

        await tx.insert(outboxEvents).values(
          dispatched.map((item) => {
            const newEmailEventId = crypto.randomUUID();
            const rootCorrelationId = String(item.correlationId || item.eventId || newEmailEventId);
            const directCausationId = String(item.eventId || rootCorrelationId);
            return {
              id: newEmailEventId,
              topic: "email-successful",
              payload: {
                eventId: newEmailEventId,
                correlationId: rootCorrelationId,
                causationId: directCausationId,
                orderId: item.orderId,
                userId: item.userId,
                emailId: item.emailId,
              },
              published: false,
            };
          })
        );
      });
    } catch (err) {
      batchFailed = true;
      batchErr = err;
    }
  }

  // 4. Fallback: if batch tx failed, process one message at a time in strict offset order
  if (batchFailed) {
    console.warn(
      `Email Service [eachBatch Fallback]: Batch transaction failed (${batchErr?.message}). Falling back to per-message processing for ${batch.messages.length} message(s)...`
    );
    let fallbackProcessed = 0;
    let fallbackSkipped = 0;

    for (const message of batch.messages) {
      if (!isRunning() || isStale()) break;
      const res = await processSingleEmailMessage({
        message,
        db,
        tables,
        maxRetries,
        retryBaseDelayMs,
        dispatchEmail,
      });
      if (res.status === "processed") fallbackProcessed++;
      else fallbackSkipped++;
      resolveOffset(message.offset);
      await heartbeat();
    }

    if (commitOffsetsIfNecessary) {
      await commitOffsetsIfNecessary();
    }

    return {
      mode: "fallback_single",
      processedCount: fallbackProcessed,
      skippedDuplicates: fallbackSkipped,
    };
  }

  for (const message of batch.messages) {
    resolveOffset(message.offset);
  }
  if (commitOffsetsIfNecessary) {
    await commitOffsetsIfNecessary();
  }
  await heartbeat();

  return {
    mode: "batch",
    processedCount: dispatched.length,
    skippedDuplicates,
  };
}
