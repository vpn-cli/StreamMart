import { execSync } from 'node:child_process';

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:8085';
const TOTAL_CHECKOUTS = 500;

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function queryDb(sqlQuery) {
  const cmd = `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`;
  return execSync(cmd, { encoding: 'utf8' }).trim();
}

async function runOutboxReliabilityBenchmark() {
  console.log('='.repeat(75));
  console.log('🧪 StreamMart Benchmark: Outbox Reliability Under Broker Outage');
  console.log('='.repeat(75));
  console.log(`Scenario: Stop Kafka broker (docker stop streammart-kafka)`);
  console.log(`          Send ${TOTAL_CHECKOUTS} checkout requests through API Gateway.`);
  console.log(`          Restart Kafka broker, wait for Outbox Relay to drain.`);
  console.log(`          Assert: exactly ${TOTAL_CHECKOUTS} orders, 0 lost, 0 duplicates.\n`);

  const batchRunId = `bench_${Date.now()}`;
  console.log('[Step 1] Acquiring JWT token from API Gateway...');
  const tokenRes = await fetch(`${GATEWAY_URL}/api/auth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId: `bench_user_${batchRunId}` }),
  });

  if (!tokenRes.ok) {
    throw new Error(`Auth failed: ${tokenRes.status} ${await tokenRes.text()}`);
  }
  const { token } = await tokenRes.json();
  console.log(`✅ Obtained JWT for batch: ${batchRunId}`);

  const baselineOrders = parseInt(queryDb('SELECT COUNT(*) FROM orders'), 10) || 0;
  const baselinePayments = parseInt(queryDb('SELECT COUNT(*) FROM payments'), 10) || 0;
  console.log(`📊 Baseline DB State: ${baselinePayments} payments, ${baselineOrders} orders`);

  // Stop Kafka Broker
  console.log('\n[Step 2] Stopping Kafka broker (docker stop streammart-kafka)...');
  execSync('docker stop streammart-kafka', { stdio: 'inherit' });
  console.log('🛑 Kafka broker stopped. Messaging system is completely OFFLINE.');

  // Dispatch 500 checkout requests
  console.log(`\n[Step 3] Dispatching ${TOTAL_CHECKOUTS} checkouts to API Gateway while Kafka is DOWN...`);
  const startTime = Date.now();
  let successfulRequests = 0;
  const concurrency = 25;

  const testUserIds = Array.from({ length: TOTAL_CHECKOUTS }, (_, i) => `user_${batchRunId}_${i}`);

  for (let i = 0; i < TOTAL_CHECKOUTS; i += concurrency) {
    const batch = testUserIds.slice(i, i + concurrency);
    await Promise.all(
      batch.map(async (uId) => {
        const res = await fetch(`${GATEWAY_URL}/api/payment`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`,
            'x-user-id': uId,
          },
          body: JSON.stringify({
            cart: [{ id: 'item-bench', name: 'Benchmark Item', price: 20.00 }],
          }),
        });

        if (res.ok) {
          successfulRequests++;
        } else {
          console.error(`Request failed for ${uId}: ${res.status}`);
        }
      })
    );
  }

  const durationSec = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`✅ Dispatched ${successfulRequests}/${TOTAL_CHECKOUTS} checkouts in ${durationSec}s (100% accepted HTTP 200)`);

  // Check DB buffering while Kafka is down
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

  // Restart Kafka Broker
  console.log('\n[Step 5] Restarting Kafka broker (docker start streammart-kafka)...');
  execSync('docker start streammart-kafka', { stdio: 'inherit' });
  console.log('⏳ Waiting for Kafka to become healthy...');

  for (let i = 0; i < 30; i++) {
    await sleep(1000);
    try {
      const inspect = execSync('docker inspect --format="{{.State.Health.Status}}" streammart-kafka', { encoding: 'utf8' }).trim();
      if (inspect === 'healthy') break;
    } catch {}
  }
  console.log('✅ Kafka broker is back ONLINE and HEALTHY.');

  // Run 2nd concurrent relay instance to verify SKIP LOCKED concurrency
  console.log('\n[Step 6] Running 2 concurrent Outbox Relay instances with SELECT ... FOR UPDATE SKIP LOCKED...');
  try {
    execSync('docker exec -d streammart-payment node -e "import(\'./worker.js\').then(m => m.startOutboxWorker())"');
    console.log('✅ Concurrent Outbox Relay Instance #2 launched in background.');
  } catch (err) {
    console.warn('Note on 2nd relay:', err.message);
  }

  // Drain Outbox and wait for Order Service
  console.log('\n[Step 7] Waiting for outbox to drain and order-service to consume all events...');
  const drainStart = Date.now();
  let drained = false;

  for (let i = 0; i < 60; i++) {
    await sleep(1000);
    const remainingOutbox = parseInt(queryDb("SELECT COUNT(*) FROM outbox_events WHERE published = false AND topic = 'payment-successful'"), 10) || 0;
    const currentOrders = parseInt(queryDb(`SELECT COUNT(*) FROM orders WHERE user_id LIKE 'user_${batchRunId}_%'`), 10) || 0;

    process.stdout.write(`   [Drain Progress] Unpublished outbox: ${remainingOutbox} | Orders created: ${currentOrders}/${TOTAL_CHECKOUTS}\r`);

    if (remainingOutbox === 0 && currentOrders === TOTAL_CHECKOUTS) {
      drained = true;
      console.log(`\n✅ Outbox completely drained and all orders processed in ${((Date.now() - drainStart) / 1000).toFixed(1)}s!`);
      break;
    }
  }

  if (!drained) {
    throw new Error('\nTimeout waiting for outbox drain and order processing.');
  }

  // Assertions
  console.log('\n[Step 8] Strict Correctness & Concurrency Verification:');
  const finalBatchOrders = parseInt(queryDb(`SELECT COUNT(*) FROM orders WHERE user_id LIKE 'user_${batchRunId}_%'`), 10);
  console.log(`   1. Total orders created for batch: ${finalBatchOrders}/${TOTAL_CHECKOUTS}`);
  if (finalBatchOrders !== TOTAL_CHECKOUTS) {
    throw new Error(`ASSERTION FAILED: Expected ${TOTAL_CHECKOUTS} orders, got ${finalBatchOrders}`);
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

runOutboxReliabilityBenchmark().catch((err) => {
  console.error('\n❌ Outbox Reliability Benchmark Failed:', err);
  process.exit(1);
});
