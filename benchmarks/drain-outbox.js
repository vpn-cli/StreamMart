import postgres from 'postgres';
import { performance } from 'node:perf_hooks';

const connectionString = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/streammart';

async function main() {
  const sql = postgres(connectionString, { max: 1, prepare: false });

  const initialRows = await sql`SELECT count(*)::int as count FROM outbox_events WHERE published = false AND topic = 'payment-successful'`;
  const initialBacklog = initialRows[0]?.count || 0;

  if (initialBacklog === 0) {
    console.log(`Outbox Backlog: 0 events (clean)`);
    await sql.end();
    return;
  }

  console.log(`Outbox Backlog: ${initialBacklog.toLocaleString()} unpublished events. Waiting for drain...`);
  const start = performance.now();

  let remaining = initialBacklog;
  while (remaining > 0) {
    await new Promise((r) => setTimeout(r, 200));
    const rows = await sql`SELECT count(*)::int as count FROM outbox_events WHERE published = false AND topic = 'payment-successful'`;
    remaining = rows[0]?.count || 0;
  }

  const elapsedSec = (performance.now() - start) / 1000;
  console.log(`✅ Outbox Drained: ${initialBacklog.toLocaleString()} events in ${elapsedSec.toFixed(2)}s (${(initialBacklog / elapsedSec).toFixed(1)} events/s relay rate)`);

  await sql.end();
}

main().catch(console.error);
