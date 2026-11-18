import postgres from 'postgres';
import { drizzle } from 'drizzle-orm/postgres-js';
import { pgTable, text, timestamp, boolean, jsonb } from 'drizzle-orm/pg-core';
import crypto from 'node:crypto';
import { performance } from 'node:perf_hooks';

const connectionString = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/streammart';
const concurrency = parseInt(process.env.CONCURRENCY || '50', 10);
const totalTransactions = parseInt(process.env.TX_COUNT || '500', 10);

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

async function testPoolSize(poolSize) {
  const client = postgres(connectionString, { max: poolSize, prepare: false });
  const db = drizzle(client);

  // Warmup 5 transactions
  for (let i = 0; i < 5; i++) {
    const eventId = crypto.randomUUID();
    await db.transaction(async (tx) => {
      await tx.insert(payments).values({ userId: 'warmup', amount: '10.00', status: 'completed' });
      await tx.insert(outboxEvents).values({ id: eventId, topic: 'warmup', payload: {}, published: false });
    });
  }

  let scheduled = 0;
  const latencies = [];
  const startAll = performance.now();

  async function worker() {
    while (scheduled < totalTransactions) {
      scheduled++;
      const start = performance.now();
      const eventId = crypto.randomUUID();
      await db.transaction(async (tx) => {
        await tx.insert(payments).values({ userId: `bench_p${poolSize}`, amount: '10.00', status: 'completed' });
        await tx.insert(outboxEvents).values({ id: eventId, topic: 'bench_pool', payload: { eventId }, published: false });
      });
      latencies.push(performance.now() - start);
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  const elapsedSec = (performance.now() - startAll) / 1000;

  latencies.sort((a, b) => a - b);
  const min = latencies[0];
  const max = latencies[latencies.length - 1];
  const p50 = latencies[Math.floor(latencies.length * 0.5)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  const mean = latencies.reduce((a, b) => a + b, 0) / latencies.length;
  const throughput = totalTransactions / elapsedSec;

  console.log(`Pool=${String(poolSize).padStart(2)}: ${throughput.toFixed(2).padStart(6)} tx/s | p50=${p50.toFixed(2).padStart(6)} ms | p95=${p95.toFixed(2).padStart(6)} ms | mean=${mean.toFixed(2).padStart(6)} ms | range=[${min.toFixed(1)}, ${max.toFixed(1)}] ms`);
  await client.end();
}

async function main() {
  console.log('========================================================================================');
  console.log(`📊 DB Connection Pool Sweep (${totalTransactions} transactions, ${concurrency} concurrency)`);
  console.log('========================================================================================');
  for (const poolSize of [5, 10, 15, 20, 25, 30, 40]) {
    await testPoolSize(poolSize);
  }
  console.log('========================================================================================');
}

main().catch(console.error);
