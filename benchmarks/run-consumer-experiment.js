import { execSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

const k6Binary = 'C:\\Program Files\\k6\\k6.exe';
const rawDir = path.resolve('benchmarks/raw');
if (!fs.existsSync(rawDir)) fs.mkdirSync(rawDir, { recursive: true });

// Supported experiments:
// 'baseline': 1 partition, eachMessage, sequential
// 'fix_a': 6 partitions, partitionsConsumedConcurrently=6, eachMessage
// 'fix_b': 1 partition, eachBatch, batch transaction
const targetExperiment = process.argv[2] || 'fix_a';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function queryDb(sqlQuery) {
  try {
    const out = execSync(
      `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`,
      { encoding: 'utf8' }
    ).trim();
    return out;
  } catch (err) {
    console.error(`DB Query Error [${sqlQuery}]:`, err.message);
    return '0';
  }
}

function resetPostgres() {
  console.log('>>> [Clean Reset] Resetting Postgres (truncating all tables and ensuring outbox partial indexes)...');
  execSync(
    'docker exec streammart-postgres psql -U postgres -d streammart -c "TRUNCATE payments, orders, processed_events, outbox_events CASCADE;"',
    { stdio: 'inherit' }
  );
  execSync(
    `docker exec streammart-postgres psql -U postgres -d streammart -c "
      CREATE INDEX IF NOT EXISTS idx_outbox_events_payment_unpublished ON outbox_events (created_at ASC) WHERE published = false AND topic = 'payment-successful';
      CREATE INDEX IF NOT EXISTS idx_outbox_events_order_unpublished ON outbox_events (created_at ASC) WHERE published = false AND topic = 'order-successful';
      CREATE INDEX IF NOT EXISTS idx_outbox_events_email_unpublished ON outbox_events (created_at ASC) WHERE published = false AND topic = 'email-successful';
    "`,
    { stdio: 'inherit' }
  );
}

async function resetKafkaTopics(numPartitions = 1) {
  console.log(`>>> [Clean Reset] Stopping app containers before Kafka topic reset...`);
  try {
    execSync('docker compose stop api-gateway payment-service order-service email-service analytic-service', { stdio: 'inherit' });
  } catch (_) { }

  console.log(`>>> [Clean Reset] Deleting and recreating Kafka application topics with ${numPartitions} partition(s)...`);
  const topics = ['payment-successful', 'order-successful', 'email-successful', 'payment-successful.DLQ'];
  for (const topic of topics) {
    try {
      execSync(
        `docker exec streammart-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${topic}`,
        { stdio: 'pipe' }
      );
    } catch (_) { }
  }

  // Wait for Kafka async topic deletion to complete
  await sleep(4000);

  for (const topic of topics) {
    execSync(
      `docker exec streammart-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic ${topic} --partitions ${numPartitions} --replication-factor 1`,
      { stdio: 'inherit' }
    );
  }

  // Verify partition count on payment-successful
  const desc = execSync(
    'docker exec streammart-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic payment-successful',
    { encoding: 'utf8' }
  );
  console.log(`>>> [Kafka Verification] payment-successful:\n${desc.trim()}`);
  if (!desc.includes(`PartitionCount: ${numPartitions}`)) {
    throw new Error(`Expected PartitionCount: ${numPartitions} for payment-successful, got:\n${desc}`);
  }
}

async function deployExperiment(experimentName) {
  console.log(`\n========================================================================`);
  console.log(`>>> [EXPERIMENT] Deploying: ${experimentName}`);
  console.log(`========================================================================`);

  let numPartitions = 1;
  let consumerMode = 'eachMessage';
  let concurrency = '1';

  if (experimentName === 'fix_a') {
    numPartitions = 6;
    consumerMode = 'eachMessage';
    concurrency = '6';
  } else if (experimentName === 'fix_b') {
    numPartitions = 1;
    consumerMode = 'eachBatch';
    concurrency = '1';
  } else if (experimentName === 'baseline_consumer') {
    numPartitions = 1;
    consumerMode = 'eachMessage';
    concurrency = '1';
  }

  // 1. Reset Kafka topics (stops app containers first) and Postgres
  await resetKafkaTopics(numPartitions);
  resetPostgres();

  // 2. Deploy services with target configuration (force-recreate to guarantee env vars apply)
  console.log(`>>> Deploying containers: consumerMode=${consumerMode}, partitionsConsumedConcurrently=${concurrency}, partitions=${numPartitions}...`);
  execSync('docker compose up -d --build --force-recreate api-gateway payment-service order-service email-service analytic-service', {
    stdio: 'inherit',
    env: {
      ...process.env,
      DB_POOL_MAX: '30',
      PROXY_KEEP_ALIVE: 'true',
      RELAY_BATCH_SEND: 'true',
      RELAY_BUSY_PAUSE_MS: '25',
      OUTBOX_RETENTION_HOURS: '1',
      PAYMENT_SERVICE_URL: 'http://payment-service:8000',
      CONSUMER_MODE: consumerMode,
      PARTITIONS_CONSUMED_CONCURRENTLY: concurrency,
    },
  });

  // 3. Wait for all services to become healthy
  console.log('>>> Waiting for all containers to report healthy...');
  const services = [
    'streammart-gateway',
    'streammart-payment',
    'streammart-order',
    'streammart-email',
    'streammart-analytic',
  ];

  for (let i = 0; i < 40; i++) {
    await sleep(2000);
    try {
      let allHealthy = true;
      for (const svc of services) {
        const status = execSync(`docker inspect ${svc} --format "{{.State.Health.Status}}"`, { encoding: 'utf8' }).trim();
        if (status !== 'healthy') {
          allHealthy = false;
          break;
        }
      }
      if (allHealthy) {
        console.log('>>> All services healthy and ready!');
        await sleep(3000);
        // Extract and print [EFFECTIVE CONFIG] startup log lines from inside each container
        const effectiveConfigs = {};
        console.log('>>> [Effective Config Verification from Container Startup Logs]');
        for (const svc of ['streammart-gateway', 'streammart-payment', 'streammart-order', 'streammart-email']) {
          const logs = execSync(`docker logs ${svc} 2>&1`, { encoding: 'utf8' });
          const configLine = logs
            .split(/\r?\n/)
            .find((line) => line.includes('[EFFECTIVE CONFIG]')) || '(not found)';
          effectiveConfigs[svc] = configLine.trim();
          console.log(`    ${svc}: ${effectiveConfigs[svc]}`);
        }
        const orderLogs = execSync('docker logs streammart-order 2>&1', { encoding: 'utf8' });
        const expectedMarker = `mode=${consumerMode}, partitionsConsumedConcurrently=${concurrency}`;
        if (!orderLogs.includes(expectedMarker)) {
          throw new Error(`Order service did not start with expected config "${expectedMarker}". Logs:\n${orderLogs.slice(-800)}`);
        }
        console.log(`>>> [Order Verification] Confirmed: ${expectedMarker}`);
        return {
          numPartitions,
          consumerMode,
          partitionsConsumedConcurrently: Number(concurrency),
          containerStartupLogs: effectiveConfigs,
        };
      }
    } catch (e) {
      if (e.message && e.message.includes('Order service did not start')) throw e;
    }
  }
  throw new Error('Timed out waiting for services to become healthy');
}

function runK6(vus, duration, summaryFile, rawFile) {
  return new Promise((resolve, reject) => {
    const args = [
      'run',
      '--vus', String(vus),
      '--duration', duration,
      '--summary-export', summaryFile,
      'benchmarks/gateway-load.js',
    ];

    const outStream = fs.createWriteStream(rawFile);
    const child = spawn(k6Binary, args, { stdio: ['inherit', 'pipe', 'pipe'] });

    child.stdout.pipe(outStream);
    child.stdout.pipe(process.stdout);
    child.stderr.pipe(process.stderr);

    child.on('close', (code) => {
      outStream.end();
      if (code !== 0) return reject(new Error(`k6 exited with code ${code}`));

      const summary = JSON.parse(fs.readFileSync(summaryFile, 'utf8'));
      const reqCount = summary.metrics.http_reqs?.count || 0;
      const rps = summary.metrics.http_reqs?.rate || 0;
      const p50 = summary.metrics.http_req_duration?.med || 0;
      const p95 = summary.metrics.http_req_duration?.['p(95)'] || 0;
      const failedRate = (summary.metrics.http_req_failed?.value || 0) * 100;

      resolve({ reqCount, rps, p50, p95, errorRate: failedRate });
    });
  });
}

async function waitForConsumerCatchup(targetOrders, ordersBefore, maxWaitMs = 120000) {
  const start = performance.now();
  let currentOrders = parseInt(queryDb('SELECT COUNT(*) FROM orders;') || '0', 10);
  let ordersProcessed = currentOrders - ordersBefore;

  console.log(`    [Catch-up] Target: ${targetOrders} orders. Currently: ${ordersProcessed}. Waiting for consumer lag -> 0...`);

  const timeout = performance.now() + maxWaitMs;
  while (ordersProcessed < targetOrders && performance.now() < timeout) {
    await sleep(250);
    currentOrders = parseInt(queryDb('SELECT COUNT(*) FROM orders;') || '0', 10);
    ordersProcessed = currentOrders - ordersBefore;
    process.stdout.write(`    [Catch-up] Lag: ${targetOrders - ordersProcessed} remaining (${ordersProcessed}/${targetOrders})\r`);
  }

  const drainSec = Number(((performance.now() - start) / 1000).toFixed(2));
  console.log(`\n    [Catch-up] Completed ${ordersProcessed}/${targetOrders} in ${drainSec}s (${(ordersProcessed / (drainSec || 0.001)).toFixed(1)} orders/s).`);

  return { ordersProcessed, drainSec, timedOut: ordersProcessed < targetOrders };
}

function calcStats(numbers) {
  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const mean = numbers.reduce((a, b) => a + b, 0) / numbers.length;
  return { mean, min, max };
}

async function runExperiment(experimentName) {
  const effectiveConfig = await deployExperiment(experimentName);

  const experimentResults = {
    _effectiveConfig: effectiveConfig,
  };

  for (const vus of [20, 50, 100]) {
    console.log(`\n------------------------------------------------------------------------`);
    console.log(`🚦 Tier: ${experimentName} @ ${vus} VUs (1 Warm-up + 3 Measured Runs)`);
    console.log(`------------------------------------------------------------------------`);

    // 1. Warm-up (discarded)
    console.log(`>>> [Warm-up] ${vus} VUs for 30s (discarded)...`);
    const warmupSummary = path.join(rawDir, `${experimentName}-${vus}vu-warmup.json`);
    const warmupRaw = path.join(rawDir, `${experimentName}-${vus}vu-warmup.txt`);
    const warmupOrdersBefore = parseInt(queryDb('SELECT COUNT(*) FROM orders;') || '0', 10);
    const warmupPaymentsBefore = parseInt(queryDb('SELECT COUNT(*) FROM payments;') || '0', 10);
    const warmupMetrics = await runK6(vus, '30s', warmupSummary, warmupRaw);
    const warmupPaymentsAt30s = parseInt(queryDb('SELECT COUNT(*) FROM payments;') || '0', 10);
    const warmupAcceptedPayments = warmupPaymentsAt30s - warmupPaymentsBefore;
    await waitForConsumerCatchup(warmupAcceptedPayments, warmupOrdersBefore, 180000);
    await sleep(2000);

    // 2. Three measured runs
    const measuredRuns = [];
    for (let r = 1; r <= 3; r++) {
      console.log(`\n>>> [Measured Run ${r}/3] ${experimentName} @ ${vus} VUs for 30s...`);
      const summaryFile = path.join(rawDir, `${experimentName}-${vus}vu-run${r}.json`);
      const rawFile = path.join(rawDir, `${experimentName}-${vus}vu-run${r}.txt`);

      // Ensure zero leftover lag before starting run
      const ordersBefore = parseInt(queryDb('SELECT COUNT(*) FROM orders;') || '0', 10);
      const paymentsBefore = parseInt(queryDb('SELECT COUNT(*) FROM payments;') || '0', 10);
      if (paymentsBefore !== ordersBefore) {
        console.warn(`    ⚠️ Warning: Prior run had leftover difference! Payments=${paymentsBefore}, Orders=${ordersBefore}. Draining completely...`);
        await waitForConsumerCatchup(paymentsBefore - ordersBefore, ordersBefore, 180000);
      }

      const runOrdersBefore = parseInt(queryDb('SELECT COUNT(*) FROM orders;') || '0', 10);
      const runPaymentsBefore = parseInt(queryDb('SELECT COUNT(*) FROM payments;') || '0', 10);
      if (runPaymentsBefore !== runOrdersBefore) {
        throw new Error(`Cannot start Measured Run ${r}: leftover lag still exists (Payments=${runPaymentsBefore}, Orders=${runOrdersBefore})`);
      }

      // Run 30s k6 load
      const k6Metrics = await runK6(vus, '30s', summaryFile, rawFile);

      // Snapshot immediately at end of 30s window to measure consumer lag
      const paymentsAt30s = parseInt(queryDb('SELECT COUNT(*) FROM payments;') || '0', 10);
      const acceptedPayments = paymentsAt30s - runPaymentsBefore;
      const ordersAt30s = parseInt(queryDb('SELECT COUNT(*) FROM orders;') || '0', 10);
      const ordersDuring30s = ordersAt30s - runOrdersBefore;
      const consumerLagAt30s = Math.max(0, acceptedPayments - ordersDuring30s);

      console.log(`\n    [30s Window Snapshot] Accepted Payments: ${acceptedPayments} | Orders Created During 30s: ${ordersDuring30s} | End-of-Run Consumer Lag: ${consumerLagAt30s}`);

      // Wait until orders count == accepted payments (Kafka consumer lag = 0)
      const catchup = await waitForConsumerCatchup(acceptedPayments, runOrdersBefore, 300000);

      const totalCompletedOrders = catchup.ordersProcessed;
      const runDurationSec = k6Metrics.reqCount / (k6Metrics.rps || 1);
      const totalTimeSec = runDurationSec + catchup.drainSec;
      const completedThroughput = totalCompletedOrders / totalTimeSec;

      console.log(`    [Run ${r} Result] Accepted: ${k6Metrics.rps.toFixed(2)} req/s | Completed: ${completedThroughput.toFixed(2)} checkouts/s (Lag reached 0 in ${catchup.drainSec}s, timedOut=${catchup.timedOut})`);

      measuredRuns.push({
        run: r,
        acceptedRate: k6Metrics.rps,
        completedRate: completedThroughput,
        acceptedCount: acceptedPayments,
        completedCount: totalCompletedOrders,
        consumerLagAt30s: consumerLagAt30s,
        timeToReachZero: catchup.drainSec,
        timedOut: catchup.timedOut,
        p50: k6Metrics.p50,
        p95: k6Metrics.p95,
        errorRate: k6Metrics.errorRate,
      });

      await sleep(2000);
    }

    const acceptedStats = calcStats(measuredRuns.map((r) => r.acceptedRate));
    const completedStats = calcStats(measuredRuns.map((r) => r.completedRate));
    const lagStats = calcStats(measuredRuns.map((r) => r.consumerLagAt30s));
    const drainStats = calcStats(measuredRuns.map((r) => r.timeToReachZero));
    const p50Stats = calcStats(measuredRuns.map((r) => r.p50));
    const p95Stats = calcStats(measuredRuns.map((r) => r.p95));

    experimentResults[vus] = {
      runs: measuredRuns,
      summary: {
        acceptedRate: acceptedStats,
        completedRate: completedStats,
        consumerLagAt30s: lagStats,
        timeToReachZero: drainStats,
        p50: p50Stats,
        p95: p95Stats,
      },
    };
  }

  const outJson = path.join(rawDir, `summary-experiment-${experimentName}.json`);
  fs.writeFileSync(outJson, JSON.stringify(experimentResults, null, 2), 'utf8');
  console.log(`\n✅ Finished experiment ${experimentName}. Summary saved to ${outJson}`);
  return experimentResults;
}

async function main() {
  const exps = targetExperiment === 'all'
    ? ['baseline_consumer', 'fix_a', 'fix_b']
    : targetExperiment === 'fixes'
      ? ['fix_a', 'fix_b']
      : [targetExperiment];

  for (const exp of exps) {
    await runExperiment(exp);
  }
}

main().catch((err) => {
  console.error('\n❌ Experiment runner error:', err);
  process.exit(1);
});
