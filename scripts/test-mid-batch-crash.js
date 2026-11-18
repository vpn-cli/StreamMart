import { execSync, execFileSync } from 'node:child_process';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function queryDb(sqlQuery) {
  return execSync(
    `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`,
    { encoding: 'utf8' }
  ).trim();
}

function getConsumerLag(groupId, topic) {
  const out = execSync(
    `docker exec streammart-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ${groupId}`,
    { encoding: 'utf8' }
  );
  let totalLag = 0;
  for (const line of out.split(/\r?\n/)) {
    const cols = line.trim().split(/\s+/);
    if (cols.length >= 6 && cols[1] === topic) {
      const lag = parseInt(cols[5], 10);
      if (!Number.isNaN(lag)) totalLag += lag;
    }
  }
  return totalLag;
}

function produceMessages(topic, lines) {
  const input = lines.join('\n') + '\n';
  execFileSync(
    'docker',
    [
      'exec', '-i', 'streammart-kafka',
      '/opt/kafka/bin/kafka-console-producer.sh',
      '--bootstrap-server', 'localhost:9092',
      '--topic', topic,
      '--property', 'parse.key=true',
      '--property', 'key.separator=|',
    ],
    { input, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
  );
}

async function waitForOrderHealthy(maxSec = 45) {
  for (let i = 0; i < maxSec; i++) {
    await sleep(1000);
    try {
      const status = execSync('docker inspect streammart-order --format "{{.State.Health.Status}}"', {
        encoding: 'utf8',
      }).trim();
      if (status === 'healthy') return;
    } catch {}
  }
  throw new Error('Timed out waiting for streammart-order to become healthy');
}

async function runMidBatchCrashTest() {
  const runId = `midbatch_${Date.now()}`;
  const BATCH_SIZE = 50;

  console.log('='.repeat(75));
  console.log('🧪 Live Integration Test: Crash Mid-Batch (After DB Commit, Before Offset Commit)');
  console.log('='.repeat(75));

  const initialLogs = execSync('docker logs streammart-order 2>&1', { encoding: 'utf8' });
  if (!initialLogs.includes('mode=eachBatch')) {
    throw new Error('order-service must be running with CONSUMER_MODE=eachBatch for this test.');
  }

  // 1. Pause order-service so 50 messages queue up into a single batch
  console.log('\n[Step 1] Stopping order-service to stage a 50-message batch in Kafka...');
  execSync('docker stop streammart-order', { stdio: 'inherit' });

  // 2. Produce 50 valid messages; mark last one with testHoldBeforeOffsetCommit=true
  //    so order-service commits the batch transaction to Postgres and pauses right before resolveOffset()
  console.log(`[Step 2] Producing ${BATCH_SIZE} valid payment-successful messages...`);
  const kafkaLines = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    const evtId = `evt_${runId}_${i}`;
    const userId = `user_${runId}_${i}`;
    const payload = {
      eventId: evtId,
      correlationId: evtId,
      causationId: evtId,
      userId,
      cart: [{ id: `item-${i}`, price: 15.0 }],
      ...(i === BATCH_SIZE - 1 ? { testHoldBeforeOffsetCommit: true } : {}),
    };
    kafkaLines.push(`${userId}|${JSON.stringify(payload)}`);
  }
  produceMessages('payment-successful', kafkaLines);
  console.log(`✅ Staged ${BATCH_SIZE} messages in payment-successful.`);

  // 3. Start order-service and wait until all 50 orders are committed in Postgres
  console.log('\n[Step 3] Starting order-service and waiting for DB batch commit...');
  execSync('docker start streammart-order', { stdio: 'inherit' });

  let ordersBeforeKill = 0;
  for (let i = 0; i < 40; i++) {
    await sleep(500);
    ordersBeforeKill = parseInt(
      queryDb(`SELECT COUNT(*) FROM orders WHERE user_id LIKE 'user_${runId}_%'`),
      10
    ) || 0;
    if (ordersBeforeKill === BATCH_SIZE) break;
  }
  if (ordersBeforeKill !== BATCH_SIZE) {
    throw new Error(`Expected ${BATCH_SIZE} orders committed before kill, saw ${ordersBeforeKill}`);
  }
  console.log(`✅ Confirmed in Postgres: ${ordersBeforeKill}/${BATCH_SIZE} orders committed.`);

  // 4. Verify Kafka offsets are STILL NOT committed (lag == 50)
  const lagBeforeKill = getConsumerLag('order-service', 'payment-successful');
  console.log(`✅ Confirmed in Kafka BEFORE kill: consumer lag = ${lagBeforeKill} (offsets NOT committed yet).`);
  if (lagBeforeKill < BATCH_SIZE) {
    throw new Error(`Expected uncommitted lag >= ${BATCH_SIZE} before SIGKILL, got ${lagBeforeKill}`);
  }

  // 5. Hard-kill order-service with SIGKILL before offsets are committed
  console.log('\n[Step 4] Sending SIGKILL to order-service (docker kill -s SIGKILL streammart-order)...');
  execSync('docker kill -s SIGKILL streammart-order', { stdio: 'inherit' });
  await sleep(1000);

  const runningState = execSync('docker inspect streammart-order --format "{{.State.Running}}"', {
    encoding: 'utf8',
  }).trim();
  const lagAfterKill = getConsumerLag('order-service', 'payment-successful');
  console.log(`   Container running=${runningState} | Kafka lag after kill=${lagAfterKill}`);
  if (runningState !== 'false') {
    throw new Error(`Expected streammart-order to be stopped after SIGKILL, got running=${runningState}`);
  }

  // 6. Restart order-service -> Kafka redelivers all 50 messages -> bulk idempotency skips duplicates -> commits offsets
  console.log('\n[Step 5] Restarting order-service (docker start streammart-order)...');
  execSync('docker start streammart-order', { stdio: 'inherit' });
  await waitForOrderHealthy();

  // Wait for Kafka lag to drop to 0 and all 50 outbox events to publish
  console.log('\n[Step 6] Waiting for redelivery dedupe, offset commit (lag -> 0), and outbox publish...');
  let finalLag = -1;
  let publishedOutbox = 0;
  for (let i = 0; i < 40; i++) {
    await sleep(500);
    finalLag = getConsumerLag('order-service', 'payment-successful');
    publishedOutbox = parseInt(
      queryDb(`SELECT COUNT(*) FROM outbox_events WHERE topic = 'order-successful' AND published = true AND payload->>'userId' LIKE 'user_${runId}_%'`),
      10
    ) || 0;
    if (finalLag === 0 && publishedOutbox === BATCH_SIZE) break;
  }

  // 7. Strict assertions
  const totalOrders = parseInt(
    queryDb(`SELECT COUNT(*) FROM orders WHERE user_id LIKE 'user_${runId}_%'`),
    10
  ) || 0;
  const distinctUsers = parseInt(
    queryDb(`SELECT COUNT(DISTINCT user_id) FROM orders WHERE user_id LIKE 'user_${runId}_%'`),
    10
  ) || 0;
  const duplicateRows = queryDb(
    `SELECT user_id, COUNT(*) FROM orders WHERE user_id LIKE 'user_${runId}_%' GROUP BY user_id HAVING COUNT(*) > 1`
  );
  const dupCount = duplicateRows ? duplicateRows.split('\n').filter(Boolean).length : 0;

  const afterLogs = execSync('docker logs streammart-order --tail 60 2>&1', { encoding: 'utf8' });
  const sawIdempotencySkip = afterLogs.includes('[eachBatch Idempotency]');

  console.log('\n[Step 7] Verification Summary:');
  console.log(`   - Orders in DB:              ${totalOrders}/${BATCH_SIZE}`);
  console.log(`   - Distinct users with order: ${distinctUsers}/${BATCH_SIZE}`);
  console.log(`   - Duplicate orders:          ${dupCount}`);
  console.log(`   - Published order outbox:    ${publishedOutbox}/${BATCH_SIZE}`);
  console.log(`   - Final Kafka consumer lag:  ${finalLag}`);
  console.log(`   - Redelivery dedupe logged:  ${sawIdempotencySkip}`);

  if (totalOrders !== BATCH_SIZE || distinctUsers !== BATCH_SIZE || dupCount !== 0) {
    throw new Error(`Order integrity failed: total=${totalOrders}, distinct=${distinctUsers}, dups=${dupCount}`);
  }
  if (publishedOutbox !== BATCH_SIZE) {
    throw new Error(`Outbox publish incomplete: ${publishedOutbox}/${BATCH_SIZE}`);
  }
  if (finalLag !== 0) {
    throw new Error(`Kafka consumer lag did not reach 0 after restart: ${finalLag}`);
  }

  console.log('\n' + '='.repeat(75));
  console.log(`🎉 PASS: Killed mid-batch with SIGKILL after DB commit; ${BATCH_SIZE}/${BATCH_SIZE} orders present, 0 duplicates, lag=0!`);
  console.log('='.repeat(75));
}

runMidBatchCrashTest().catch((err) => {
  console.error('\n❌ Mid-Batch Crash Test Failed:', err);
  process.exit(1);
});
