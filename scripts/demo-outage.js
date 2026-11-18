import { execSync } from 'node:child_process';

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:8085';
const CHECKOUT_COUNT = parseInt(process.env.DEMO_CHECKOUT_COUNT || '20', 10);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, options, maxRetries = 4) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fetch(url, options);
    } catch (err) {
      if (attempt === maxRetries) throw err;
      await sleep(150 * attempt);
    }
  }
}

function queryDb(sqlQuery) {
  return execSync(
    `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`,
    { encoding: 'utf8' }
  ).trim();
}

function probePaymentEndpoint(endpoint) {
  try {
    const out = execSync(
      `docker exec streammart-payment node -e "fetch('http://localhost:8000${endpoint}').then(async r => console.log(r.status + '|' + (await r.text())))"`,
      { encoding: 'utf8' }
    ).trim();
    const idx = out.indexOf('|');
    return { status: parseInt(out.slice(0, idx), 10), body: out.slice(idx + 1) };
  } catch (err) {
    return { status: 0, body: err.message };
  }
}

async function waitForKafkaHealthy(maxSec = 45) {
  process.stdout.write('⏳ Waiting for Kafka healthcheck');
  for (let i = 0; i < maxSec; i++) {
    await sleep(1000);
    process.stdout.write('.');
    try {
      const status = execSync('docker inspect --format "{{.State.Health.Status}}" streammart-kafka', { encoding: 'utf8' }).trim();
      if (status === 'healthy') {
        console.log(' healthy!\n');
        return;
      }
    } catch {}
  }
  console.log('');
  throw new Error('Timed out waiting for streammart-kafka to become healthy');
}

async function runDemo() {
  const demoId = `demo_${Date.now()}`;
  let kafkaStopped = false;

  console.log('\n' + '='.repeat(76));
  console.log('🎬  StreamMart Live Demo: Surviving a Complete Kafka Broker Outage');
  console.log('='.repeat(76));
  console.log('This narrated demo proves the Transactional Outbox pattern end-to-end:');
  console.log('  1. Stop the Kafka broker completely');
  console.log(`  2. Send ${CHECKOUT_COUNT} checkouts through the API Gateway while Kafka is down`);
  console.log('  3. Show every payment saved & queued in Postgres (0 orders yet)');
  console.log('  4. Restart Kafka and watch every queued checkout drain and complete\n');

  try {
    // Step 0: Verify API Gateway and Kafka are healthy before starting
    console.log('[Step 0] Checking that the local Docker stack is running...');
    const healthRes = await fetchWithRetry(`${GATEWAY_URL}/health`).catch(() => null);
    if (!healthRes || !healthRes.ok) {
      throw new Error(`API Gateway not reachable at ${GATEWAY_URL}. Run "npm run stack:up" first.`);
    }
    const kafkaStatus = execSync('docker inspect --format "{{.State.Health.Status}}" streammart-kafka', { encoding: 'utf8' }).trim();
    if (kafkaStatus !== 'healthy') {
      await waitForKafkaHealthy();
    }
    console.log(`✅ Stack is healthy (API Gateway at ${GATEWAY_URL}, Kafka healthy).\n`);

    // Step 1: Mint demo JWTs for test shoppers
    console.log(`[Step 1] Minting ${CHECKOUT_COUNT} demo shopper JWTs via /api/auth/token (demo-only endpoint)...`);
    const users = [];
    for (let i = 0; i < CHECKOUT_COUNT; i++) {
      const userId = `${demoId}_shopper_${i + 1}`;
      const res = await fetchWithRetry(`${GATEWAY_URL}/api/auth/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId }),
      });
      if (!res.ok) {
        throw new Error(`Failed to mint demo token (${res.status}). Ensure DEMO_AUTH=true and NODE_ENV!=production.`);
      }
      const { token } = await res.json();
      users.push({ userId, token });
    }
    console.log(`✅ Ready with ${users.length} shopper tokens.\n`);

    // Step 2: Stop Kafka
    console.log('[Step 2] Pulling the plug on Apache Kafka (docker stop streammart-kafka)...');
    execSync('docker stop streammart-kafka', { stdio: 'inherit' });
    kafkaStopped = true;
    await sleep(1000);
    const liveness = probePaymentEndpoint('/health');
    console.log('🛑 Kafka is now OFFLINE.');
    console.log(`   - payment-service /health (liveness): HTTP ${liveness.status} (checkout API stays UP)\n`);

    // Step 3: Send checkouts while Kafka is down
    console.log(`[Step 3] Sending ${CHECKOUT_COUNT} customer checkouts while Kafka is completely DOWN...`);
    let accepted = 0;
    const startMs = Date.now();
    for (let i = 0; i < users.length; i += 5) {
      const chunk = users.slice(i, i + 5);
      await Promise.all(
        chunk.map(async ({ token }, idx) => {
          const res = await fetchWithRetry(`${GATEWAY_URL}/api/payment`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify({
              cart: [{ id: `demo-sku-${i + idx + 1}`, name: 'Outbox Demo Item', price: 29.99 }],
            }),
          });
          if (res && res.ok) accepted++;
        })
      );
    }
    const elapsedMs = Date.now() - startMs;
    console.log(`✅ Accepted ${accepted}/${CHECKOUT_COUNT} checkouts with HTTP 200 in ${elapsedMs}ms despite Kafka being offline!\n`);

    // Step 4: Inspect PostgreSQL during outage
    console.log('[Step 4] Inspecting PostgreSQL while Kafka is STILL OFFLINE:');
    const paymentsSaved = parseInt(queryDb(`SELECT COUNT(*) FROM payments WHERE user_id LIKE '${demoId}_%'`), 10) || 0;
    const outboxPending = parseInt(queryDb(`SELECT COUNT(*) FROM outbox_events WHERE topic = 'payment-successful' AND published = false AND payload->>'userId' LIKE '${demoId}_%'`), 10) || 0;
    const ordersWhileDown = parseInt(queryDb(`SELECT COUNT(*) FROM orders WHERE user_id LIKE '${demoId}_%'`), 10) || 0;

    console.log(`   - Payments committed in Postgres:            ${paymentsSaved}/${CHECKOUT_COUNT}`);
    console.log(`   - Queued outbox rows (published = false):    ${outboxPending}/${CHECKOUT_COUNT}`);
    console.log(`   - Orders created while Kafka is down:        ${ordersWhileDown} (expected 0 — safely buffered in DB)\n`);

    // Step 5: Restart Kafka
    console.log('[Step 5] Bringing Apache Kafka back online (docker start streammart-kafka)...');
    execSync('docker start streammart-kafka', { stdio: 'inherit' });
    kafkaStopped = false;
    await waitForKafkaHealthy();

    // Step 6: Watch events drain and deliver end-to-end
    console.log('[Step 6] Watching the outbox relay reconnect and deliver all queued checkouts...');
    const drainStart = Date.now();
    let ordersCreated = 0;
    let emailsQueued = 0;
    let remainingOutbox = outboxPending;

    for (let i = 0; i < 120; i++) {
      await sleep(500);
      const snap = queryDb(
        `SELECT ` +
        `(SELECT COUNT(*) FROM outbox_events WHERE topic = 'payment-successful' AND published = false AND payload->>'userId' LIKE '${demoId}_%') || '|' || ` +
        `(SELECT COUNT(*) FROM orders WHERE user_id LIKE '${demoId}_%') || '|' || ` +
        `(SELECT COUNT(*) FROM outbox_events WHERE topic = 'email-successful' AND payload->>'userId' LIKE '${demoId}_%')`
      );
      [remainingOutbox, ordersCreated, emailsQueued] = snap.split('|').map((n) => parseInt(n, 10) || 0);
      process.stdout.write(
        `   Outbox remaining: ${remainingOutbox} | Orders created: ${ordersCreated}/${CHECKOUT_COUNT} | Emails queued: ${emailsQueued}/${CHECKOUT_COUNT}   \r`
      );
      if (remainingOutbox === 0 && ordersCreated === CHECKOUT_COUNT && emailsQueued === CHECKOUT_COUNT) {
        break;
      }
    }
    const drainSec = ((Date.now() - drainStart) / 1000).toFixed(2);
    console.log(`\n✅ Full recovery & end-to-end delivery completed in ${drainSec}s!\n`);

    const dupRows = queryDb(
      `SELECT user_id FROM orders WHERE user_id LIKE '${demoId}_%' GROUP BY user_id HAVING COUNT(*) > 1`
    );
    const duplicates = dupRows ? dupRows.split('\n').filter(Boolean).length : 0;
    const lost = Math.max(0, CHECKOUT_COUNT - ordersCreated);
    const relayAfter = probePaymentEndpoint('/relay-status');

    console.log('='.repeat(76));
    console.log('📊  Demo Summary');
    console.log('='.repeat(76));
    console.log(`   Checkouts sent while Kafka was down:  ${CHECKOUT_COUNT}`);
    console.log(`   HTTP 200 accepted during outage:      ${accepted}/${CHECKOUT_COUNT}`);
    console.log(`   Orders created after Kafka recovery:  ${ordersCreated}/${CHECKOUT_COUNT}`);
    console.log(`   Emails queued after Kafka recovery:   ${emailsQueued}/${CHECKOUT_COUNT}`);
    console.log(`   Orders lost:                          ${lost}`);
    console.log(`   Duplicate orders:                     ${duplicates}`);
    console.log(`   payment-service /relay-status now:    HTTP ${relayAfter.status}`);
    console.log('='.repeat(76) + '\n');

    if (accepted !== CHECKOUT_COUNT || ordersCreated !== CHECKOUT_COUNT || duplicates !== 0) {
      throw new Error('Demo verification failed!');
    }
  } finally {
    if (kafkaStopped) {
      console.log('Restoring Kafka broker before exiting...');
      try {
        execSync('docker start streammart-kafka', { stdio: 'inherit' });
      } catch {}
    }
  }
}

runDemo().catch((err) => {
  console.error('\n❌ Demo failed:', err.message);
  process.exit(1);
});
