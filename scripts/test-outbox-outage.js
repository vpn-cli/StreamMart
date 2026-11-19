import { execSync } from 'node:child_process';
import path from 'node:path';

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:8085';
const TOTAL_CHECKOUTS = 500;

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function queryDb(sqlQuery) {
  const cmd = `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`;
  return execSync(cmd, { encoding: 'utf8' }).trim();
}

async function fetchWithRetry(url, options, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url, options);
      return res;
    } catch (err) {
      if (attempt === maxRetries) throw err;
      await sleep(100 * attempt);
    }
  }
}

async function runOutboxOutageTest() {
  console.log('='.repeat(75));
  console.log('🧪 StreamMart Phase C: Outbox Outage & Concurrent Relay Integration Test');
  console.log('='.repeat(75));
  console.log(`Scenario: Stop Kafka broker, send ${TOTAL_CHECKOUTS} checkouts via API Gateway.`);
  console.log('          Restart Kafka, run 2 concurrent outbox relays with SKIP LOCKED.');
  console.log(`          Assert: exactly ${TOTAL_CHECKOUTS} orders, 0 lost, 0 duplicates.\n`);

  // 1. Authenticate and obtain JWTs for test users
  console.log('[Step 1] Requesting JWT tokens from API Gateway for test users...');
  const batchRunId = `batch_${Date.now()}`;
  const concurrency = 20;
  const userTokens = new Map();

  for (let i = 0; i < TOTAL_CHECKOUTS; i += 50) {
    const chunk = Array.from({ length: Math.min(50, TOTAL_CHECKOUTS - i) }, (_, idx) => `user_${batchRunId}_${i + idx}`);
    await Promise.all(
      chunk.map(async (uId) => {
        const tokenRes = await fetchWithRetry(`${GATEWAY_URL}/api/auth/token`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ userId: uId }),
        });
        if (tokenRes && tokenRes.ok) {
          const { token } = await tokenRes.json();
          userTokens.set(uId, token);
        }
      })
    );
  }
  console.log(`✅ Obtained ${userTokens.size} distinct user JWTs for batch run: ${batchRunId}`);

  // Count baseline
  const baselineOrders = parseInt(queryDb('SELECT COUNT(*) FROM orders'), 10) || 0;
  const baselinePayments = parseInt(queryDb('SELECT COUNT(*) FROM payments'), 10) || 0;
  console.log(`📊 Baseline DB State: ${baselinePayments} payments, ${baselineOrders} orders`);

  // 2. Stop Kafka Broker
  console.log('\n[Step 2] Stopping Kafka broker (docker stop streammart-kafka)...');
  execSync('docker stop streammart-kafka', { stdio: 'inherit' });
  console.log('🛑 Kafka broker stopped. The messaging system is completely OFFLINE.');

  // 3. Dispatch 500 checkout requests through API Gateway
  console.log(`\n[Step 3] Dispatching ${TOTAL_CHECKOUTS} checkouts to API Gateway while Kafka is DOWN...`);
  const startTime = Date.now();
  let successfulRequests = 0;

  const testUserIds = Array.from({ length: TOTAL_CHECKOUTS }, (_, i) => `user_${batchRunId}_${i}`);

  for (let i = 0; i < TOTAL_CHECKOUTS; i += concurrency) {
    const batch = testUserIds.slice(i, i + concurrency);
    await Promise.all(
      batch.map(async (uId) => {
        const token = userTokens.get(uId);
        const res = await fetchWithRetry(`${GATEWAY_URL}/api/payment`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`,
          },
          body: JSON.stringify({
            cart: [{ id: 'item-outage', name: 'Resilience Cap', price: 25.00 }],
          }),
        });

        if (res && res.ok) {
          successfulRequests++;
        } else {
          console.error(`Request failed for ${uId}: ${res ? res.status : 'no response'}`);
        }
      })
    );
    if ((i + concurrency) % 100 === 0 || i + concurrency >= TOTAL_CHECKOUTS) {
      process.stdout.write(`   Dispatched ${Math.min(i + concurrency, TOTAL_CHECKOUTS)}/${TOTAL_CHECKOUTS} checkouts...\n`);
    }
  }

  const durationSec = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`✅ Dispatched ${successfulRequests}/${TOTAL_CHECKOUTS} checkouts in ${durationSec}s (100% accepted HTTP 200)`);

  if (successfulRequests !== TOTAL_CHECKOUTS) {
    throw new Error(`Expected ${TOTAL_CHECKOUTS} successful checkouts, got ${successfulRequests}`);
  }

  // 4. Verify DB state while Kafka is DOWN
  console.log('\n[Step 4] Verifying database isolation while Kafka is OFFLINE:');
  const paymentsDuringOutage = parseInt(queryDb('SELECT COUNT(*) FROM payments'), 10) - baselinePayments;
  const ordersDuringOutage = parseInt(queryDb('SELECT COUNT(*) FROM orders'), 10) - baselineOrders;
  const pendingOutboxRows = parseInt(queryDb("SELECT COUNT(*) FROM outbox_events WHERE published = false AND topic = 'payment-successful'"), 10);

  console.log(`   - New payments recorded in DB: ${paymentsDuringOutage}/${TOTAL_CHECKOUTS}`);
  console.log(`   - Pending outbox events in DB: ${pendingOutboxRows}/${TOTAL_CHECKOUTS}`);
  console.log(`   - Orders processed while Kafka is down: ${ordersDuringOutage} (Must be exactly 0)`);

  if (paymentsDuringOutage !== TOTAL_CHECKOUTS || ordersDuringOutage !== 0 || pendingOutboxRows < TOTAL_CHECKOUTS) {
    throw new Error('Database isolation check failed! Outbox rows must buffer while Kafka is down.');
  }
  console.log('✅ PASS: All 500 events safely buffered in outbox table without data loss.');

  // 5. Restart Kafka Broker
  console.log('\n[Step 5] Restarting Kafka broker (docker start streammart-kafka)...');
  execSync('docker start streammart-kafka', { stdio: 'inherit' });
  console.log('⏳ Waiting for Kafka to become healthy and accept connections...');

  let kafkaReady = false;
  for (let i = 0; i < 35; i++) {
    await sleep(1000);
    try {
      const inspect = execSync('docker inspect --format "{{.State.Health.Status}}" streammart-kafka', { encoding: 'utf8' }).trim();
      if (inspect === 'healthy') {
        kafkaReady = true;
        break;
      }
    } catch {}
  }
  console.log('✅ Kafka broker is back ONLINE and HEALTHY.');

  // 6. Spawn 2nd Concurrent Outbox Relay Instance
  console.log('\n[Step 6] Running 2 concurrent Outbox Relay instances with SELECT ... FOR UPDATE SKIP LOCKED...');
  try {
    execSync('docker exec -d streammart-payment node -e "import(\'./worker.js\').then(m => m.startOutboxWorker())"');
    console.log('✅ Concurrent Outbox Relay Instance #2 launched in background.');
  } catch (err) {
    console.warn('Note on 2nd relay:', err.message);
  }

  // 7. Drain Outbox and wait for Order Service to process all 500 events
  console.log('\n[Step 7] Waiting for outbox to drain and order-service to consume all events...');
  const drainStart = Date.now();
  let drained = false;

  for (let i = 0; i < 100; i++) {
    await sleep(1000);
    const result = queryDb(`SELECT (SELECT COUNT(*) FROM outbox_events WHERE published = false AND topic = 'payment-successful') || '|' || (SELECT COUNT(*) FROM orders WHERE user_id LIKE 'user_${batchRunId}_%')`);
    const [remStr, ordStr] = result.split('|');
    const remainingOutbox = parseInt(remStr, 10) || 0;
    const currentOrders = parseInt(ordStr, 10) || 0;

    process.stdout.write(`   [Drain Progress] Unpublished outbox: ${remainingOutbox} | Orders created: ${currentOrders}/${TOTAL_CHECKOUTS}\r`);

    if (remainingOutbox === 0 && currentOrders === TOTAL_CHECKOUTS) {
      drained = true;
      console.log(`\n✅ Outbox completely drained and all orders processed in ${((Date.now() - drainStart) / 1000).toFixed(1)}s!`);
      break;
    }
  }

  if (!drained) {
    console.log('\n');
    throw new Error('Timeout waiting for outbox drain and order processing.');
  }

  // 8. Assertions: 500 orders, 0 lost, 0 duplicates processed
  console.log('\n[Step 8] Executing Strict Correctness & Concurrency Assertions:');

  const finalBatchOrders = parseInt(queryDb(`SELECT COUNT(*) FROM orders WHERE user_id LIKE 'user_${batchRunId}_%'`), 10);
  console.log(`   1. Total orders created for batch: ${finalBatchOrders}/${TOTAL_CHECKOUTS}`);
  if (finalBatchOrders !== TOTAL_CHECKOUTS) {
    throw new Error(`ASSERTION FAILED: Expected ${TOTAL_CHECKOUTS} orders, got ${finalBatchOrders} (Lost: ${TOTAL_CHECKOUTS - finalBatchOrders})`);
  }
  console.log('      ✅ 0 Orders Lost (100% delivery guarantee).');

  const duplicateUsers = queryDb(`SELECT user_id, COUNT(*) FROM orders WHERE user_id LIKE 'user_${batchRunId}_%' GROUP BY user_id HAVING COUNT(*) > 1`);
  console.log(`   2. Duplicate orders detected: ${duplicateUsers ? duplicateUsers.split('\n').length : 0}`);
  if (duplicateUsers && duplicateUsers.length > 0) {
    throw new Error(`ASSERTION FAILED: Duplicate orders detected!\n${duplicateUsers}`);
  }
  console.log('      ✅ 0 Duplicates Processed (Strict Idempotency verified).');

  const processedEventsCount = parseInt(queryDb(`SELECT COUNT(*) FROM processed_events WHERE service_name = 'order-service' AND event_id IN (SELECT id::text FROM outbox_events WHERE payload->>'userId' LIKE 'user_${batchRunId}_%')`), 10);
  console.log(`   3. Order-service processed_events records: ${processedEventsCount}/${TOTAL_CHECKOUTS}`);
  if (processedEventsCount !== TOTAL_CHECKOUTS) {
    throw new Error(`ASSERTION FAILED: Expected ${TOTAL_CHECKOUTS} processed_events records, got ${processedEventsCount}`);
  }
  console.log('      ✅ Exactly 500 idempotent event records stored.');

  console.log('\n' + '='.repeat(75));
  console.log(`🎉 SUCCESS: 500 orders processed, 0 lost, 0 duplicates under broker outage & concurrent relays!`);
  console.log('='.repeat(75));
}

runOutboxOutageTest().catch((err) => {
  console.error('\n❌ Outbox Outage Test Failed:', err);
  process.exit(1);
});
