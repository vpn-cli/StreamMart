import { performance } from 'node:perf_hooks';
import { execSync } from 'node:child_process';

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:8085';

function queryDb(sqlQuery) {
  const cmd = `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`;
  return execSync(cmd, { encoding: 'utf8' }).trim();
}

async function measureLatency() {
  console.log('='.repeat(70));
  console.log('📊 StreamMart Latency Benchmark: Synchronous Gateway & Asynchronous Event');
  console.log('='.repeat(70));

  // 1. Authenticate with Gateway
  console.log('\n[Phase 1] Acquiring JWT for synchronous HTTP benchmark...');
  const authRes = await fetch(`${GATEWAY_URL}/api/auth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId: 'latency_benchmark_user' }),
  });

  if (!authRes.ok) {
    throw new Error(`Failed to acquire JWT: ${authRes.status} ${await authRes.text()}`);
  }

  const { token } = await authRes.json();
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${token}`,
  };

  const payload = JSON.stringify({
    cart: [
      { id: 'item_bench_1', name: 'Latency Benchmark Hoodie', price: 49.99 },
      { id: 'item_bench_2', name: 'Microservices Mug', price: 12.50 },
    ],
  });

  // 2. Measure First Request (Cold Path)
  console.log('\n--- 1. Measuring Synchronous HTTP Gateway Latency ---');
  const t0 = performance.now();
  const firstRes = await fetch(`${GATEWAY_URL}/api/payment`, {
    method: 'POST',
    headers,
    body: payload,
  });
  const firstLatency = performance.now() - t0;
  if (!firstRes.ok) throw new Error(`First request failed: ${firstRes.status}`);
  console.log(`First Request (Cold Path):      ${firstLatency.toFixed(2)} ms`);

  // 3. Steady-State Requests (30 sequential requests)
  const latencies = [];
  for (let i = 1; i <= 30; i++) {
    const start = performance.now();
    const res = await fetch(`${GATEWAY_URL}/api/payment`, {
      method: 'POST',
      headers,
      body: payload,
    });
    const dur = performance.now() - start;
    latencies.push(dur);
    if (!res.ok) throw new Error(`Request #${i} failed: ${res.status}`);
  }

  latencies.sort((a, b) => a - b);
  const min = latencies[0];
  const max = latencies[latencies.length - 1];
  const sum = latencies.reduce((a, b) => a + b, 0);
  const avg = sum / latencies.length;
  const p50 = latencies[Math.floor(latencies.length * 0.5)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  const p99 = latencies[Math.floor(latencies.length * 0.99)];

  console.log(`Steady-State Min:               ${min.toFixed(2)} ms`);
  console.log(`Steady-State Max:               ${max.toFixed(2)} ms`);
  console.log(`Steady-State Avg (Mean):        ${avg.toFixed(2)} ms`);
  console.log(`Steady-State p50 (Median):      ${p50.toFixed(2)} ms`);
  console.log(`Steady-State p95:               ${p95.toFixed(2)} ms`);
  console.log(`Steady-State p99:               ${p99.toFixed(2)} ms`);

  // 4. Measure End-to-End Asynchronous Event Latency (Kafka + Outbox -> Consumer)
  console.log('\n--- 2. Measuring End-to-End Asynchronous Event Latency ---');
  console.log('Definition: Time from order-service database commit (orders.created_at)');
  console.log('            to downstream email-service processing (processed_events.processed_at).\n');

  // Let event consumer catch up for 2 seconds
  await new Promise((r) => setTimeout(r, 2000));

  const sqlQuery = "SELECT COUNT(*) AS total_samples, ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (pe.processed_at - o.created_at)) * 1000)::numeric, 2) AS p50_ms, ROUND(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (pe.processed_at - o.created_at)) * 1000)::numeric, 2) AS p95_ms, ROUND(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (pe.processed_at - o.created_at)) * 1000)::numeric, 2) AS p99_ms, ROUND(AVG(EXTRACT(EPOCH FROM (pe.processed_at - o.created_at)) * 1000)::numeric, 2) AS avg_ms FROM orders o JOIN outbox_events oe ON oe.payload->>'orderId' = o.id::text JOIN processed_events pe ON pe.event_id = oe.id::text WHERE pe.service_name = 'email-service';";
  const e2eStatsRaw = queryDb(sqlQuery);

  const [totalSamples, e2eP50, e2eP95, e2eP99, e2eAvg] = e2eStatsRaw.split('|');

  console.log(`Total Events Sampled:           ${totalSamples}`);
  console.log(`End-to-End Event Latency Avg:   ${e2eAvg} ms`);
  console.log(`End-to-End Event Latency p50:   ${e2eP50} ms`);
  console.log(`End-to-End Event Latency p95:   ${e2eP95} ms`);
  console.log(`End-to-End Event Latency p99:   ${e2eP99} ms`);

  console.log('\n' + '='.repeat(70));
  console.log('✅ Latency Measurement Complete');
  console.log('='.repeat(70));
}

measureLatency().catch((err) => {
  console.error('Latency measurement failed:', err);
  process.exit(1);
});
