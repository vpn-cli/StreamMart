import { execSync } from 'node:child_process';
import { performance } from 'node:perf_hooks';

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:8085';
const NUM_SAMPLES = 50;

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function queryDb(sqlQuery) {
  const cmd = `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`;
  return execSync(cmd, { encoding: 'utf8' }).trim();
}

async function measureE2ELatency() {
  console.log('='.repeat(70));
  console.log(`⏱️ StreamMart: Measuring End-to-End Event Latency (${NUM_SAMPLES} sequential runs)`);
  console.log('   Latency = (Time from HTTP Checkout Dispatch -> Order Row Created in DB)');
  console.log('='.repeat(70));

  const runId = Date.now();
  console.log('Pre-generating user auth tokens...');
  const tokens = [];

  for (let i = 1; i <= NUM_SAMPLES; i++) {
    const userId = `user_e2e_${runId}_${i}`;
    const tokenRes = await fetch(`${GATEWAY_URL}/api/auth/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId }),
    });
    const { token } = await tokenRes.json();
    tokens.push({ userId, token });
  }
  console.log(`✅ Pre-generated ${tokens.length} user JWTs.\n`);

  const latencies = [];

  for (let i = 0; i < tokens.length; i++) {
    const { userId, token } = tokens[i];
    const sampleNum = i + 1;
    const t0 = performance.now();

    // 1. Dispatch checkout
    const res = await fetch(`${GATEWAY_URL}/api/payment`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
      },
      body: JSON.stringify({
        cart: [{ id: `item-${sampleNum}`, name: 'Benchmark Item', price: 15.00 }],
      }),
    });

    if (!res.ok) {
      console.warn(`Request #${sampleNum} failed`);
      continue;
    }

    // 2. Poll DB until order is committed by order-service
    let orderCommitted = false;
    let tEnd = 0;
    for (let poll = 0; poll < 100; poll++) {
      await sleep(20);
      const count = parseInt(queryDb(`SELECT COUNT(*) FROM orders WHERE user_id = '${userId}'`), 10) || 0;
      if (count > 0) {
        tEnd = performance.now();
        orderCommitted = true;
        break;
      }
    }

    if (orderCommitted) {
      const e2eDuration = tEnd - t0;
      latencies.push(e2eDuration);
      console.log(`Sample #${sampleNum.toString().padStart(2, ' ')}: ${e2eDuration.toFixed(2)} ms`);
    } else {
      console.warn(`Sample #${sampleNum}: timed out waiting for order commit`);
    }
  }

  // Calculate statistics
  const sorted = [...latencies].sort((a, b) => a - b);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  const avg = sum / sorted.length;
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)];
  const p99 = sorted[Math.floor(sorted.length * 0.99)];

  console.log('\n' + '='.repeat(70));
  console.log('              END-TO-END EVENT LATENCY RESULTS               ');
  console.log('='.repeat(70));
  console.log(`Sample Count:          ${sorted.length}`);
  console.log(`Min Latency:           ${min.toFixed(2)} ms`);
  console.log(`Max Latency:           ${max.toFixed(2)} ms`);
  console.log(`Mean (Average):        ${avg.toFixed(2)} ms`);
  console.log(`Median (p50):          ${p50.toFixed(2)} ms`);
  console.log(`95th Percentile (p95): ${p95.toFixed(2)} ms`);
  console.log(`99th Percentile (p99): ${p99.toFixed(2)} ms`);
  console.log('='.repeat(70) + '\n');
}

measureE2ELatency().catch((err) => {
  console.error('E2E Latency Benchmark failed:', err);
  process.exit(1);
});
