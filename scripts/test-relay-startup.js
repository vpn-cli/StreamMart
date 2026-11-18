import { execSync } from 'node:child_process';

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function queryDb(sqlQuery) {
  const cmd = `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`;
  return execSync(cmd, { encoding: 'utf8' }).trim();
}

function queryPaymentService(script) {
  const cmd = `docker exec streammart-payment node -e "${script}"`;
  return execSync(cmd, { encoding: 'utf8' }).trim();
}

async function runTest() {
  console.log('='.repeat(75));
  console.log('🧪 Relay Startup Integration Test: Kafka Down -> Startup -> Up -> Deliver');
  console.log('='.repeat(75));

  const testUserId = `user_startup_${Date.now()}`;
  console.log(`Test user: ${testUserId}`);

  // Step 1: Baseline
  const baselinePayments = parseInt(queryDb('SELECT COUNT(*) FROM payments'), 10) || 0;
  const baselineOrders = parseInt(queryDb('SELECT COUNT(*) FROM orders'), 10) || 0;
  console.log(`[Step 1] Baseline DB State: ${baselinePayments} payments, ${baselineOrders} orders`);

  // Step 2: Stop Kafka
  console.log('\n[Step 2] Stopping Kafka broker (docker stop streammart-kafka)...');
  execSync('docker stop streammart-kafka', { stdio: 'inherit' });
  console.log('🛑 Kafka broker stopped.');

  // Step 3: Restart payment-service with Kafka DOWN
  console.log('\n[Step 3] Restarting payment-service with Kafka DOWN (docker restart streammart-payment)...');
  execSync('docker restart streammart-payment', { stdio: 'inherit' });
  await sleep(3000);

  // Step 4: Verify payment health check returns 503 while Kafka is down
  console.log('\n[Step 4] Probing payment-service /health while Kafka is down...');
  const healthCheckScript = `
    fetch('http://localhost:8000/health')
      .then(async r => console.log(JSON.stringify({ status: r.status, text: await r.text() })))
      .catch(e => console.log(JSON.stringify({ error: e.message })));
  `.replace(/\n\s+/g, ' ');

  const healthResRaw = queryPaymentService(healthCheckScript);
  console.log(`   Health response: ${healthResRaw}`);
  const healthRes = JSON.parse(healthResRaw);

  if (healthRes.status !== 503) {
    throw new Error(`Expected health check 503 while Kafka is down, got: ${healthRes.status}`);
  }
  console.log('✅ PASS: Health check returned 503 Service Unavailable because relay loop is not running.');

  // Step 5: Submit payment while Kafka is down
  console.log('\n[Step 5] Submitting checkout to payment-service while Kafka is down...');
  const paymentScript = `
    fetch('http://localhost:8000/payment', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-user-id': '${testUserId}' },
      body: JSON.stringify({ cart: [{ price: 49.99 }] })
    })
      .then(async r => console.log(JSON.stringify({ status: r.status, text: await r.text() })))
      .catch(e => console.log(JSON.stringify({ error: e.message })));
  `.replace(/\n\s+/g, ' ');

  const paymentResRaw = queryPaymentService(paymentScript);
  console.log(`   Payment response: ${paymentResRaw}`);
  const paymentRes = JSON.parse(paymentResRaw);

  if (paymentRes.status !== 200) {
    throw new Error(`Expected payment HTTP 200, got: ${paymentRes.status}`);
  }
  console.log('✅ PASS: Payment accepted (HTTP 200) and atomically queued in outbox table.');

  // Step 6: Verify DB isolation while Kafka is down
  console.log('\n[Step 6] Verifying database isolation while Kafka is down...');
  const unpubCount = parseInt(
    queryDb(`SELECT COUNT(*) FROM outbox_events WHERE payload->>'userId' = '${testUserId}' AND published = false`),
    10
  );
  const ordersWhileDown = parseInt(
    queryDb(`SELECT COUNT(*) FROM orders WHERE user_id = '${testUserId}'`),
    10
  );

  console.log(`   - Unpublished outbox row for user: ${unpubCount} (Expected: 1)`);
  console.log(`   - Orders created for user while Kafka down: ${ordersWhileDown} (Expected: 0)`);

  if (unpubCount !== 1 || ordersWhileDown !== 0) {
    throw new Error('Database isolation check failed!');
  }
  console.log('✅ PASS: Event safely buffered in outbox table with published = false.');

  // Step 7: Bring Kafka up
  console.log('\n[Step 7] Bringing Kafka UP (docker start streammart-kafka)...');
  execSync('docker start streammart-kafka', { stdio: 'inherit' });
  console.log('⏳ Waiting for Kafka to become healthy...');

  let kafkaReady = false;
  for (let i = 0; i < 45; i++) {
    await sleep(1000);
    try {
      const inspectData = JSON.parse(execSync('docker inspect streammart-kafka', { encoding: 'utf8' }));
      const status = inspectData[0]?.State?.Health?.Status;
      if (status === 'healthy') {
        kafkaReady = true;
        break;
      }
    } catch (_) {}
  }

  if (!kafkaReady) {
    throw new Error('Kafka container failed to become healthy within 45s');
  }
  console.log('✅ Kafka broker is back ONLINE and HEALTHY.');

  // Step 8: Verify payment health check transitions to 200 as retry connects
  console.log('\n[Step 8] Waiting for payment relay retry loop to connect and health check to return 200...');
  let health200 = false;
  for (let i = 0; i < 20; i++) {
    await sleep(1000);
    try {
      const res = JSON.parse(queryPaymentService(healthCheckScript));
      if (res.status === 200) {
        health200 = true;
        console.log(`   Health check returned 200 OK after ${i + 1}s! Message: "${res.text}"`);
        break;
      }
    } catch (_) {}
  }

  if (!health200) {
    throw new Error('Payment service health check failed to transition to 200 OK after Kafka restart');
  }
  console.log('✅ PASS: Health check successfully transitioned to 200 OK upon Kafka reconnection.');

  // Step 9: Confirm outbox delivery and order creation
  console.log('\n[Step 9] Confirming event delivery and downstream order processing...');
  let delivered = false;
  for (let i = 0; i < 25; i++) {
    await sleep(1000);
    const pubStatus = queryDb(
      `SELECT published FROM outbox_events WHERE payload->>'userId' = '${testUserId}' LIMIT 1`
    );
    const orderCreated = parseInt(
      queryDb(`SELECT COUNT(*) FROM orders WHERE user_id = '${testUserId}'`),
      10
    );

    process.stdout.write(`   [Waiting] outbox published: ${pubStatus} | orders created: ${orderCreated}\r`);
    if (pubStatus === 't' && orderCreated === 1) {
      delivered = true;
      console.log(`\n✅ PASS: Event published to Kafka (published = true) and order-service created order for ${testUserId}!`);
      break;
    }
  }

  if (!delivered) {
    console.log('\n');
    throw new Error('Timeout waiting for event delivery or order creation');
  }

  console.log('\n' + '='.repeat(75));
  console.log('🎉 SUCCESS: Relay startup test complete! Kafka connect retry with backoff,');
  console.log('   503 health check under outage, and zero-loss event delivery verified!');
  console.log('='.repeat(75));
}

runTest().catch((err) => {
  console.error('\n❌ Relay Startup Test FAILED:', err);
  process.exit(1);
});
