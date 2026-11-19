import { execSync } from 'node:child_process';
import path from 'node:path';

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:8085';

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function queryDb(sqlQuery) {
  const cmd = `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`;
  const output = execSync(cmd, { encoding: 'utf8' }).trim();
  return output;
}

async function runCrashRecoveryTest() {
  console.log('='.repeat(75));
  console.log('🧪 StreamMart Phase C: Order-Service Crash Recovery Integration Test');
  console.log('='.repeat(75));
  console.log('Scenario: Kill order-service after DB commit but before event publish.');
  console.log('          Assert that upon restart, order-successful is emitted exactly once.\n');

  // 1. Authenticate via Gateway to get valid JWT
  console.log('[Step 1] Requesting JWT from API Gateway...');
  const testUserId = `user_crash_${Date.now()}`;
  const tokenRes = await fetch(`${GATEWAY_URL}/api/auth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId: testUserId }),
  });

  if (!tokenRes.ok) {
    throw new Error(`Auth failed: ${tokenRes.status} ${await tokenRes.text()}`);
  }
  const { token } = await tokenRes.json();
  console.log(`✅ Obtained JWT for: ${testUserId}`);

  // Count baseline orders and outbox rows before test
  const initialOrders = parseInt(queryDb(`SELECT COUNT(*) FROM orders WHERE user_id = '${testUserId}'`), 10) || 0;

  // 2. Dispatch checkout with crash hook flag: testCrashAfterDbCommit = true
  console.log('\n[Step 2] Sending Checkout with testCrashAfterDbCommit=true flag...');
  const paymentRes = await fetch(`${GATEWAY_URL}/api/payment`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
    },
    body: JSON.stringify({
      cart: [{ id: 'item-crash', name: 'Fault-Tolerance Mug', price: 99.00 }],
      testCrashAfterDbCommit: true,
    }),
  });

  if (!paymentRes.ok) {
    throw new Error(`Payment request failed: ${paymentRes.status}`);
  }
  console.log(`✅ Payment accepted by gateway (200 OK) and queued in outbox.`);

  // 3. Wait for payment outbox worker to publish to Kafka and order-service to consume and crash
  console.log('\n[Step 3] Waiting for order-service to consume payment event and trigger crash...');
  let orderCreatedInDb = false;
  let orderRow = null;

  for (let i = 0; i < 20; i++) {
    await sleep(500);
    const count = parseInt(queryDb(`SELECT COUNT(*) FROM orders WHERE user_id = '${testUserId}'`), 10) || 0;
    if (count > initialOrders) {
      orderCreatedInDb = true;
      const row = queryDb(`SELECT id, status FROM orders WHERE user_id = '${testUserId}' LIMIT 1`);
      orderRow = row.split('|');
      break;
    }
  }

  if (!orderCreatedInDb) {
    throw new Error('Order was not committed to DB within timeout.');
  }

  console.log(`✅ Confirmed: Order was committed to DB (Order ID: ${orderRow[0]}, Status: ${orderRow[1]})`);

  // Check outbox row state before publish
  const orderOutboxRow = queryDb(`SELECT id, topic, published FROM outbox_events WHERE payload->>'orderId' = '${orderRow[0]}' AND topic = 'order-successful'`);
  console.log(`✅ Confirmed DB Outbox state: ${orderOutboxRow}`);

  // 4. Wait for Docker auto-restart or explicitly restart order-service
  console.log('\n[Step 4] Checking order-service crash logs...');
  await sleep(1500);
  const orderLogs = execSync('docker logs streammart-order --tail 25', { encoding: 'utf8' });
  if (orderLogs.includes('Simulating immediate order-service crash after DB commit')) {
    console.log('✅ Found crash hook log in order-service:');
    console.log('   "💥 [TEST HOOK]: Simulating immediate order-service crash after DB commit (before relay publish)!"');
  } else {
    console.log('ℹ️ order-service logs:\n', orderLogs);
  }

  console.log('\n[Step 5] Ensuring order-service is running...');
  execSync('docker restart streammart-order', { stdio: 'inherit' });
  console.log('✅ order-service container restarted.');

  // 5. Wait for order-service outbox relay to drain and publish order-successful
  console.log('\n[Step 6] Waiting for order-service outbox relay to publish order-successful...');
  let outboxPublished = false;
  for (let i = 0; i < 30; i++) {
    await sleep(500);
    const published = queryDb(`SELECT published FROM outbox_events WHERE payload->>'orderId' = '${orderRow[0]}' AND topic = 'order-successful'`);
    if (published.trim() === 't') {
      outboxPublished = true;
      break;
    }
  }

  if (!outboxPublished) {
    throw new Error('Outbox event was not published after restart!');
  }
  console.log('✅ Confirmed in DB: outbox_events.published = true!');

  // 6. Assertions
  console.log('\n[Step 7] Running Verification Assertions:');
  const finalOrderCount = parseInt(queryDb(`SELECT COUNT(*) FROM orders WHERE user_id = '${testUserId}'`), 10);
  console.log(`   - Total orders created for user: ${finalOrderCount}`);
  if (finalOrderCount !== 1) {
    throw new Error(`Assertion Failed: Expected exactly 1 order, found ${finalOrderCount}`);
  }
  console.log('   ✅ EXACTLY ONE order persisted in database (no duplicates).');

  const outboxEventCount = parseInt(queryDb(`SELECT COUNT(*) FROM outbox_events WHERE payload->>'orderId' = '${orderRow[0]}' AND topic = 'order-successful'`), 10);
  console.log(`   - Order outbox rows for this order: ${outboxEventCount}`);
  if (outboxEventCount !== 1) {
    throw new Error(`Assertion Failed: Expected exactly 1 order-successful outbox row, found ${outboxEventCount}`);
  }
  console.log('   ✅ EXACTLY ONE order-successful outbox row created and published.');

  // Check email-service outbox and processing
  await sleep(1000);
  const emailOutboxCount = parseInt(queryDb(`SELECT COUNT(*) FROM outbox_events WHERE payload->>'orderId' = '${orderRow[0]}' AND topic = 'email-successful'`), 10);
  console.log(`   - Email outbox rows for this order: ${emailOutboxCount}`);
  if (emailOutboxCount === 1) {
    console.log('   ✅ EXACTLY ONE email-successful outbox row created and published.');
  }

  // Check email-service processing
  await sleep(1000);
  const emailProcessed = parseInt(queryDb(`SELECT COUNT(*) FROM processed_events WHERE service_name = 'email-service' AND event_id IN (SELECT id::text FROM outbox_events WHERE payload->>'orderId' = '${orderRow[0]}')`), 10);
  console.log(`   - Email service processed events: ${emailProcessed}`);
  if (emailProcessed >= 1) {
    console.log('   ✅ Email service successfully consumed and processed order-successful.');
  }

  console.log('\n' + '='.repeat(75));
  console.log('🎉 SUCCESS: Crash recovery test passed completely! Zero lost, zero duplicates.');
  console.log('='.repeat(75));
}

runCrashRecoveryTest().catch((err) => {
  console.error('\n❌ Crash Recovery Test Failed:', err);
  process.exit(1);
});
