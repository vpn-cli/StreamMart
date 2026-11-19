import postgres from 'postgres';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pgTable, text, timestamp, boolean, jsonb } from 'drizzle-orm/pg-core';
import crypto from 'node:crypto';
import { performance } from 'node:perf_hooks';

const connectionString = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/streammart';
const iters = parseInt(process.argv[2] || '100', 10);

const payments = pgTable('payments', {
  id: text('id').primaryKey().$defaultFn(() => crypto.randomUUID()),
  userId: text('user_id').notNull(),
  amount: text('amount').notNull(),
  status: text('status').notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

const outboxEvents = pgTable('outbox_events', {
  id: text('id').primaryKey(),
  topic: text('topic').notNull(),
  payload: jsonb('payload').notNull(),
  published: boolean('published').default(false).notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

async function main() {
  const client = postgres(connectionString, { max: 5, prepare: false });
  const db = drizzle(client);

  console.log('===============================================================');
  console.log(`⏱️ Isolated Payment DB Transaction Benchmark (${iters} iterations)`);
  console.log('===============================================================');

  // Warmup 5 transactions
  for (let i = 0; i < 5; i++) {
    const eventId = crypto.randomUUID();
    await db.transaction(async (tx) => {
      await tx.insert(payments).values({ userId: 'warmup', amount: '10.00', status: 'completed' });
      await tx.insert(outboxEvents).values({ id: eventId, topic: 'warmup', payload: {}, published: false });
    });
  }

  const times = [];
  for (let i = 0; i < iters; i++) {
    const start = performance.now();
    const eventId = crypto.randomUUID();
    await db.transaction(async (tx) => {
      await tx.insert(payments).values({ userId: 'bench_isolated', amount: '29.99', status: 'completed' });
      await tx.insert(outboxEvents).values({ id: eventId, topic: 'payment-successful', payload: { eventId }, published: false });
    });
    times.push(performance.now() - start);
  }

  times.sort((a, b) => a - b);
  const min = times[0];
  const max = times[times.length - 1];
  const p50 = times[Math.floor(times.length * 0.5)];
  const p95 = times[Math.floor(times.length * 0.95)];
  const mean = times.reduce((a, b) => a + b, 0) / times.length;

  console.log(`Results:`);
  console.log(`  Min:   ${min.toFixed(2)} ms`);
  console.log(`  Mean:  ${mean.toFixed(2)} ms`);
  console.log(`  p50:   ${p50.toFixed(2)} ms`);
  console.log(`  p95:   ${p95.toFixed(2)} ms`);
  console.log(`  Max:   ${max.toFixed(2)} ms`);
  console.log('===============================================================');

  await client.end();
}

main().catch(console.error);
