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

function readAllDlqMessages() {
  try {
    const out = execSync(
      'docker exec streammart-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic payment-successful.DLQ --from-beginning --timeout-ms 4000',
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
    );
    return out
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l.startsWith('{'))
      .map((l) => JSON.parse(l));
  } catch (err) {
    const stdout = err.stdout ? err.stdout.toString() : '';
    return stdout
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l.startsWith('{'))
      .map((l) => JSON.parse(l));
  }
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

async function runPoisonBatchTest() {
  const runId = `poison_${Date.now()}`;
  const poisonEventId = `poison_evt_${runId}`;
  const BATCH_SIZE = 50;
  const POISON_INDEX = 24;

  console.log('='.repeat(75));
  console.log('🧪 Live Integration Test: Poison Message in eachBatch (1 bad among 50)');
  console.log('='.repeat(75));

  // Verify order-service is in eachBatch mode
  const initialLogs = execSync('docker logs streammart-order 2>&1', { encoding: 'utf8' });
  if (!initialLogs.includes('mode=eachBatch')) {
    throw new Error('order-service must be running with CONSUMER_MODE=eachBatch for this test.');
  }

  // 1. Stop order-service briefly so all 50 messages queue up and arrive as one batch
  console.log('\n[Step 1] Pausing order-service so 50 messages accumulate as a single batch...');
  execSync('docker stop streammart-order', { stdio: 'inherit' });

  // 2. Produce 49 valid messages + 1 poison message (userId: null -> PG 23502 NOT NULL violation)
  console.log(`[Step 2] Producing ${BATCH_SIZE} messages to payment-successful (index ${POISON_INDEX} has userId=null)...`);
  const kafkaLines = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    if (i === POISON_INDEX) {
      const payload = {
        eventId: poisonEventId,
        correlationId: poisonEventId,
        causationId: poisonEventId,
        userId: null,
        cart: [{ id: 'poison-item', price: 10 }],
      };
      kafkaLines.push(`poison-key|${JSON.stringify(payload)}`);
    } else {
      const evtId = `valid_evt_${runId}_${i}`;
      const userId = `user_${runId}_${i}`;
      const payload = {
        eventId: evtId,
        correlationId: evtId,
        causationId: evtId,
        userId,
        cart: [{ id: `item-${i}`, price: 19.99 }],
      };
      kafkaLines.push(`${userId}|${JSON.stringify(payload)}`);
    }
  }
  produceMessages('payment-successful', kafkaLines);
  console.log(`✅ Produced ${BATCH_SIZE} messages (49 valid + 1 poison).`);

  // 3. Start order-service so eachBatch fetches the batch
  console.log('\n[Step 3] Starting order-service (eachBatch mode)...');
  execSync('docker start streammart-order', { stdio: 'inherit' });
  await waitForOrderHealthy();

  // 4. Wait for 49 orders to be created
  console.log('\n[Step 4] Waiting for batch fallback and 49 valid orders to commit...');
  let createdOrders = 0;
  for (let i = 0; i < 40; i++) {
    await sleep(500);
    createdOrders = parseInt(
      queryDb(`SELECT COUNT(*) FROM orders WHERE user_id LIKE 'user_${runId}_%'`),
      10
    ) || 0;
    if (createdOrders === 49) break;
  }
  console.log(`   Orders created for batch: ${createdOrders}/49`);
  if (createdOrders !== 49) {
    throw new Error(`Expected exactly 49 orders, got ${createdOrders}`);
  }

  // 5. Verify exactly 1 message landed in payment-successful.DLQ for this run
  console.log('\n[Step 5] Checking payment-successful.DLQ for the poison message...');
  const dlqAll = readAllDlqMessages();
  const matchingDlq = dlqAll.filter((m) => m.eventId === poisonEventId);
  console.log(`   Matching DLQ messages for ${poisonEventId}: ${matchingDlq.length}`);
  if (matchingDlq.length !== 1) {
    throw new Error(`Expected exactly 1 DLQ message for ${poisonEventId}, found ${matchingDlq.length}`);
  }
  console.log(`   DLQ error recorded: ${matchingDlq[0].error}`);

  // 6. Verify fallback log occurred
  const afterLogs = execSync('docker logs streammart-order --tail 80 2>&1', { encoding: 'utf8' });
  const sawFallback = afterLogs.includes('[eachBatch Fallback]');
  console.log(`   Saw [eachBatch Fallback] in order-service logs: ${sawFallback}`);
  if (!sawFallback) {
    throw new Error('Expected [eachBatch Fallback] log message proving batch tx failed and fell back 1-by-1');
  }

  // 7. Verify consumer is NOT stuck: send 1 follow-up healthy message (#51)
  console.log('\n[Step 6] Sending follow-up healthy message #51 to prove consumer is NOT stuck...');
  const followUpUser = `user_${runId}_followup`;
  const followUpEvt = `evt_${runId}_followup`;
  produceMessages('payment-successful', [
    `${followUpUser}|${JSON.stringify({
      eventId: followUpEvt,
      correlationId: followUpEvt,
      causationId: followUpEvt,
      userId: followUpUser,
      cart: [{ id: 'followup-item', price: 42 }],
    })}`,
  ]);

  let followUpOrders = 0;
  for (let i = 0; i < 20; i++) {
    await sleep(500);
    followUpOrders = parseInt(
      queryDb(`SELECT COUNT(*) FROM orders WHERE user_id = '${followUpUser}'`),
      10
    ) || 0;
    if (followUpOrders === 1) break;
  }
  if (followUpOrders !== 1) {
    throw new Error(`Consumer appears stuck: follow-up message was not processed (orders=${followUpOrders})`);
  }

  const finalLag = getConsumerLag('order-service', 'payment-successful');
  console.log(`   Follow-up order created: ${followUpOrders}/1 | Kafka consumer lag: ${finalLag}`);
  if (finalLag !== 0) {
    throw new Error(`Expected Kafka consumer lag 0, got ${finalLag}`);
  }

  console.log('\n' + '='.repeat(75));
  console.log('🎉 PASS: 49 orders created, 1 poison message in DLQ, follow-up #51 processed, lag=0!');
  console.log('='.repeat(75));
}

runPoisonBatchTest().catch((err) => {
  console.error('\n❌ Poison Batch Test Failed:', err);
  process.exit(1);
});
