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

export async function processAnalyticBatch({
  batch,
  resolveOffset,
  heartbeat,
  commitOffsetsIfNecessary,
  isRunning,
  isStale,
  db,
  tables,
}) {
  if (!isRunning() || isStale()) {
    return { mode: "skipped_not_running", processedCount: 0, skippedDuplicates: 0 };
  }

  const { processedEvents } = tables;
  const parsedItems = [];

  for (const message of batch.messages) {
    const raw = message.value ? message.value.toString() : "";
    try {
      const data = JSON.parse(raw);
      parsedItems.push({
        message,
        data,
        eventId: data.eventId == null ? "" : String(data.eventId),
      });
    } catch (parseErr) {
      console.error(`Analytic Service [JSON Error] topic=${batch.topic} offset=${message.offset}:`, parseErr.message);
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
            eq(processedEvents.serviceName, "analytic-service")
          )
        );
      existingSet = new Set(existing.map((e) => e.eventId));
    } catch (e) {
      console.error("Analytic Service: Bulk idempotency check failed:", e.message);
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

  // 2. Single bulk insert into processed_events for all new events in this batch
  const toInsert = newItems
    .filter((item) => item.eventId)
    .map((item) => ({
      eventId: item.eventId,
      serviceName: "analytic-service",
    }));

  if (db && toInsert.length > 0) {
    try {
      await db.insert(processedEvents).values(toInsert).onConflictDoNothing();
    } catch (batchErr) {
      console.warn(
        `Analytic Service [eachBatch Fallback]: Bulk insert failed (${batchErr.message}). Processing ${toInsert.length} event(s) individually...`
      );
      for (const row of toInsert) {
        try {
          await db.insert(processedEvents).values(row).onConflictDoNothing();
        } catch (singleErr) {
          if (!isDuplicateConstraintError(singleErr)) {
            console.error(`Analytic Service: Failed to record event ${row.eventId}:`, singleErr.message);
          }
        }
      }
    }
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
    processedCount: newItems.length,
    skippedDuplicates,
  };
}
