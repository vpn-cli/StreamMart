import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function queryDb(sql) {
  return execSync(`docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sql}"`, { encoding: 'utf8' }).trim();
}

async function main() {
  console.log('='.repeat(75));
  console.log('🔬 Benchmarking Order-Service Message Processing Breakdown (N=500)');
  console.log('='.repeat(75));

  // 1. Reset tables
  console.log('>>> [1/4] Resetting DB tables...');
  queryDb('TRUNCATE payments, orders, processed_events, outbox_events CASCADE;');

  // 2. Publish 500 payment-successful events to Kafka
  console.log('>>> [2/4] Publishing 500 test payment-successful events directly to Kafka...');
  const publishScript = `
    import { Kafka } from 'kafkajs';
    import crypto from 'node:crypto';

    const kafka = new Kafka({ clientId: 'perf-publisher', brokers: ['kafka:9092'] });
    const producer = kafka.producer();
    await producer.connect();

    const messages = [];
    for (let i = 1; i <= 500; i++) {
      const eventId = crypto.randomUUID();
      const userId = 'perf_user_' + i;
      messages.push({
        key: userId,
        value: JSON.stringify({
          eventId,
          correlationId: eventId,
          causationId: eventId,
          userId,
          cart: [{ price: 29.99, title: 'Item ' + i }],
          amount: '29.99'
        })
      });
    }

    await producer.send({ topic: 'payment-successful', messages });
    await producer.disconnect();
    console.log('Sent 500 messages to payment-successful');
  `;

  execSync(`docker exec streammart-payment node --input-type=module -e "${publishScript.replace(/\n\s+/g, ' ')}"`, { stdio: 'inherit' });

  // 3. Wait for order-service to process all 500 messages
  console.log('>>> [3/4] Waiting for order-service to consume all 500 messages...');
  let processed = 0;
  const timeout = Date.now() + 30000;
  while (Date.now() < timeout) {
    await sleep(250);
    processed = parseInt(queryDb('SELECT COUNT(*) FROM orders;') || '0', 10);
    process.stdout.write(`    Orders created: ${processed}/500\r`);
    if (processed >= 500) break;
  }
  console.log(`\n    Finished consuming: ${processed} orders created.`);

  // 4. Fetch metrics from order-service HTTP endpoint
  console.log('>>> [4/4] Fetching high-resolution instrumentation metrics from order-service...');
  const statsRaw = execSync(
    `docker exec streammart-order node -e "fetch('http://localhost:8001/consumer-stats').then(r=>r.text()).then(console.log)"`,
    { encoding: 'utf8' }
  ).trim();

  const stats = JSON.parse(statsRaw);
  console.log('\n' + '='.repeat(75));
  console.log('📊 EMPIRICAL ORDER-SERVICE CONSUMER BREAKDOWN');
  console.log('='.repeat(75));
  console.log(`Sample Count: ${stats.sampleCount} messages`);
  console.log(`Total Time per Message: avg=${stats.total.avg} ms | p50=${stats.total.p50} ms | p95=${stats.total.p95} ms (min=${stats.total.min}ms, max=${stats.total.max}ms)`);
  console.log(`  ├─ Atomic DB Transaction: avg=${stats.tx.avg} ms | p50=${stats.tx.p50} ms | p95=${stats.tx.p95} ms [${((stats.tx.avg / stats.total.avg) * 100).toFixed(1)}% of total]`);
  console.log(`  ├─ Idempotency SELECT:    avg=${stats.idempotency.avg} ms | p50=${stats.idempotency.p50} ms | p95=${stats.idempotency.p95} ms [${((stats.idempotency.avg / stats.total.avg) * 100).toFixed(1)}% of total]`);
  console.log(`  └─ CPU / JSON Parsing:    avg=${stats.overhead.avg} ms | p50=${stats.overhead.p50} ms | p95=${stats.overhead.p95} ms [${((stats.overhead.avg / stats.total.avg) * 100).toFixed(1)}% of total]`);
  console.log(`\nImplied Single-Partition Sequential Max Throughput:`);
  console.log(`  1,000 ms ÷ ${stats.total.avg} ms = ${stats.impliedThroughput} orders/sec`);
  console.log('='.repeat(75));

  const outPath = path.resolve('benchmarks/raw/consumer-bottleneck-evidence.json');
  fs.writeFileSync(outPath, JSON.stringify(stats, null, 2), 'utf8');
  console.log(`Saved empirical breakdown to ${outPath}`);
}

main().catch(console.error);
