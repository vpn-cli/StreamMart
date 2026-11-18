import { execSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

const k6Binary = 'C:\\Program Files\\k6\\k6.exe';
const rawDir = path.resolve('benchmarks/raw');
if (!fs.existsSync(rawDir)) fs.mkdirSync(rawDir, { recursive: true });

const label = process.argv[2] || 'fix_b_downstream_before';
const emailConsumerMode = process.env.EMAIL_CONSUMER_MODE || 'eachMessage';
const analyticConsumerMode = process.env.ANALYTIC_CONSUMER_MODE || 'eachMessage';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function queryDb(sqlQuery) {
  const out = execSync(
    `docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "${sqlQuery}"`,
    { encoding: 'utf8' }
  ).trim();
  return out;
}

function snapshotCounts() {
  const raw = queryDb(
    "SELECT " +
    "(SELECT COUNT(*) FROM payments) || '|' || " +
    "(SELECT COUNT(*) FROM orders) || '|' || " +
    "(SELECT COUNT(*) FROM processed_events WHERE service_name = 'email-service') || '|' || " +
    "(SELECT COUNT(*) FROM processed_events WHERE service_name = 'analytic-service') || '|' || " +
    "(SELECT COUNT(*) FROM outbox_events WHERE topic = 'order-successful' AND published = false) || '|' || " +
    "(SELECT COUNT(*) FROM outbox_events WHERE topic = 'email-successful' AND published = false)"
  );
  const [payments, orders, emailProcessed, analyticProcessed, orderOutboxPending, emailOutboxPending] =
    raw.split('|').map((v) => parseInt(v, 10) || 0);
  return {
    payments,
    orders,
    emailProcessed,
    analyticProcessed,
    orderOutboxPending,
    emailOutboxPending,
  };
}

function resetPostgres() {
  console.log('>>> [Clean Reset] Truncating Postgres tables and verifying partial indexes...');
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

async function resetKafkaTopics() {
  console.log('>>> [Clean Reset] Stopping app containers before Kafka topic reset...');
  try {
    execSync('docker compose stop api-gateway payment-service order-service email-service analytic-service', { stdio: 'inherit' });
  } catch {}

  const topics = ['payment-successful', 'order-successful', 'email-successful', 'payment-successful.DLQ'];
  for (const topic of topics) {
    try {
      execSync(
        `docker exec streammart-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${topic}`,
        { stdio: 'pipe' }
      );
    } catch {}
  }

  await sleep(4000);

  for (const topic of topics) {
    execSync(
      `docker exec streammart-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic ${topic} --partitions 1 --replication-factor 1`,
      { stdio: 'inherit' }
    );
  }
}

async function deployStack() {
  await resetKafkaTopics();
  resetPostgres();

  console.log(`>>> Deploying fix_b stack (order=eachBatch, email=${emailConsumerMode}, analytic=${analyticConsumerMode})...`);
  execSync('docker compose up -d --build --force-recreate api-gateway payment-service order-service email-service analytic-service', {
    stdio: 'inherit',
    env: {
      ...process.env,
      DB_POOL_MAX: '30',
      PROXY_KEEP_ALIVE: 'true',
      RELAY_BATCH_SEND: 'true',
      RELAY_BUSY_PAUSE_MS: '25',
      OUTBOX_RETENTION_HOURS: '1',
      CONSUMER_MODE: 'eachBatch',
      PARTITIONS_CONSUMED_CONCURRENTLY: '1',
      EMAIL_CONSUMER_MODE: emailConsumerMode,
      ANALYTIC_CONSUMER_MODE: analyticConsumerMode,
    },
  });

  const services = [
    'streammart-gateway',
    'streammart-payment',
    'streammart-order',
    'streammart-email',
    'streammart-analytic',
  ];

  for (let i = 0; i < 45; i++) {
    await sleep(2000);
    let allHealthy = true;
    for (const svc of services) {
      try {
        const status = execSync(`docker inspect ${svc} --format "{{.State.Health.Status}}"`, { encoding: 'utf8' }).trim();
        if (status !== 'healthy') {
          allHealthy = false;
          break;
        }
      } catch {
        allHealthy = false;
        break;
      }
    }
    if (allHealthy) {
      await sleep(3000);
      const effectiveConfigs = {};
      for (const svc of services) {
        const logs = execSync(`docker logs ${svc} 2>&1`, { encoding: 'utf8' });
        const line = logs.split(/\r?\n/).find((l) => l.includes('[EFFECTIVE CONFIG]')) || '(not found)';
        effectiveConfigs[svc] = line.trim();
        console.log(`    ${svc}: ${effectiveConfigs[svc]}`);
      }
      return effectiveConfigs;
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
      // Strip setup_data (contains ephemeral demo JWTs) before keeping summary on disk
      if (summary.setup_data) {
        delete summary.setup_data;
        fs.writeFileSync(summaryFile, JSON.stringify(summary, null, 2), 'utf8');
      }

      const reqCount = summary.metrics.http_reqs?.count || 0;
      const rps = summary.metrics.http_reqs?.rate || 0;
      const p50 = summary.metrics.http_req_duration?.med || 0;
      const p95 = summary.metrics.http_req_duration?.['p(95)'] || 0;
      const failedRate = (summary.metrics.http_req_failed?.value || 0) * 100;

      resolve({ reqCount, rps, p50, p95, errorRate: failedRate });
    });
  });
}

async function waitForAllCatchup(before, acceptedPayments, maxWaitMs = 300000) {
  const start = performance.now();
  const targetOrders = acceptedPayments;
  const targetEmails = acceptedPayments;
  const targetAnalytics = acceptedPayments * 3;

  let orderCatchupSec = null;
  let emailCatchupSec = null;
  let analyticCatchupSec = null;

  let snap = snapshotCounts();
  let ordersDone = snap.orders - before.orders;
  let emailsDone = snap.emailProcessed - before.emailProcessed;
  let analyticsDone = snap.analyticProcessed - before.analyticProcessed;

  const lagAt30s = {
    orderLag: Math.max(0, targetOrders - ordersDone),
    emailLag: Math.max(0, targetEmails - emailsDone),
    analyticLag: Math.max(0, targetAnalytics - analyticsDone),
    orderOutboxPending: snap.orderOutboxPending,
    emailOutboxPending: snap.emailOutboxPending,
  };

  if (ordersDone >= targetOrders) orderCatchupSec = 0;
  if (emailsDone >= targetEmails) emailCatchupSec = 0;
  if (analyticsDone >= targetAnalytics) analyticCatchupSec = 0;

  console.log(
    `    [30s Snapshot] Payments=${acceptedPayments} | OrderLag=${lagAt30s.orderLag} (orderOutbox=${lagAt30s.orderOutboxPending}) | EmailLag=${lagAt30s.emailLag} (emailOutbox=${lagAt30s.emailOutboxPending}) | AnalyticLag=${lagAt30s.analyticLag}`
  );

  const deadline = performance.now() + maxWaitMs;
  while (
    (orderCatchupSec === null || emailCatchupSec === null || analyticCatchupSec === null) &&
    performance.now() < deadline
  ) {
    await sleep(250);
    const elapsedSec = Number(((performance.now() - start) / 1000).toFixed(2));
    snap = snapshotCounts();
    ordersDone = snap.orders - before.orders;
    emailsDone = snap.emailProcessed - before.emailProcessed;
    analyticsDone = snap.analyticProcessed - before.analyticProcessed;

    if (orderCatchupSec === null && ordersDone >= targetOrders) orderCatchupSec = elapsedSec;
    if (emailCatchupSec === null && emailsDone >= targetEmails) emailCatchupSec = elapsedSec;
    if (analyticCatchupSec === null && analyticsDone >= targetAnalytics) analyticCatchupSec = elapsedSec;

    process.stdout.write(
      `    [Drain ${elapsedSec}s] Ord=${ordersDone}/${targetOrders} | Email=${emailsDone}/${targetEmails} (ordOut=${snap.orderOutboxPending}) | Analytic=${analyticsDone}/${targetAnalytics} (emOut=${snap.emailOutboxPending})   \r`
    );
  }

  const totalElapsed = Number(((performance.now() - start) / 1000).toFixed(2));
  console.log('');
  console.log(
    `    [Catch-up Done] Order=${orderCatchupSec ?? `TIMEOUT(${totalElapsed}s, rem=${targetOrders - ordersDone})`}s | Email=${emailCatchupSec ?? `TIMEOUT(${totalElapsed}s, rem=${targetEmails - emailsDone})`}s | Analytic=${analyticCatchupSec ?? `TIMEOUT(${totalElapsed}s, rem=${targetAnalytics - analyticsDone})`}s`
  );

  return {
    lagAt30s,
    orderCatchupSec: orderCatchupSec ?? totalElapsed,
    emailCatchupSec: emailCatchupSec ?? totalElapsed,
    analyticCatchupSec: analyticCatchupSec ?? totalElapsed,
    timedOut: {
      order: orderCatchupSec === null,
      email: emailCatchupSec === null,
      analytic: analyticCatchupSec === null,
    },
    finalCounts: {
      ordersDone,
      emailsDone,
      analyticsDone,
      targetOrders,
      targetEmails,
      targetAnalytics,
    },
  };
}

function calcStats(arr) {
  return {
    mean: Number((arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(2)),
    min: Number(Math.min(...arr).toFixed(2)),
    max: Number(Math.max(...arr).toFixed(2)),
  };
}

async function main() {
  const effectiveConfigs = await deployStack();
  const results = {
    label,
    emailConsumerMode,
    analyticConsumerMode,
    effectiveConfigs,
    tiers: {},
  };

  for (const vus of [50, 100]) {
    console.log(`\n========================================================================`);
    console.log(`🚦 Downstream Lag Measurement: ${label} @ ${vus} VUs (1 Warm-up + 3 Measured)`);
    console.log(`========================================================================`);

    // Warm-up (discarded, reset cleanly after warmup so zero leftover lag)
    console.log(`>>> [Warm-up] ${vus} VUs for 30s...`);
    const warmBefore = snapshotCounts();
    await runK6(
      vus,
      '30s',
      path.join(rawDir, `${label}-${vus}vu-warmup.json`),
      path.join(rawDir, `${label}-${vus}vu-warmup.txt`)
    );
    const warmSnap = snapshotCounts();
    const warmAccepted = warmSnap.payments - warmBefore.payments;
    const warmDrain = await waitForAllCatchup(warmBefore, warmAccepted, 240000);
    if (warmDrain.timedOut.email || warmDrain.timedOut.analytic) {
      console.log('    Warm-up hit timeout draining downstream; resetting topics & DB cleanly before measured runs...');
      await deployStack();
    }
    await sleep(2000);

    const runs = [];
    for (let r = 1; r <= 3; r++) {
      // Ensure zero leftover lag across all consumers before run
      const pre = snapshotCounts();
      if (pre.orders !== pre.payments || pre.emailProcessed !== pre.payments || pre.analyticProcessed !== pre.payments * 3) {
        console.log(`    Leftover lag detected before Run ${r} (P=${pre.payments}, O=${pre.orders}, E=${pre.emailProcessed}, A=${pre.analyticProcessed}); resetting cleanly...`);
        await deployStack();
      }
      const before = snapshotCounts();

      console.log(`\n>>> [Measured Run ${r}/3] ${label} @ ${vus} VUs for 30s...`);
      const k6 = await runK6(
        vus,
        '30s',
        path.join(rawDir, `${label}-${vus}vu-run${r}.json`),
        path.join(rawDir, `${label}-${vus}vu-run${r}.txt`)
      );

      const post30s = snapshotCounts();
      const acceptedPayments = post30s.payments - before.payments;
      const drain = await waitForAllCatchup(before, acceptedPayments, 300000);

      runs.push({
        run: r,
        acceptedRate: Number(k6.rps.toFixed(2)),
        acceptedPayments,
        p50: Number(k6.p50.toFixed(2)),
        p95: Number(k6.p95.toFixed(2)),
        errorRate: Number(k6.errorRate.toFixed(2)),
        lagAt30s: drain.lagAt30s,
        orderCatchupSec: drain.orderCatchupSec,
        emailCatchupSec: drain.emailCatchupSec,
        analyticCatchupSec: drain.analyticCatchupSec,
        timedOut: drain.timedOut,
        finalCounts: drain.finalCounts,
      });

      await sleep(2000);
    }

    results.tiers[vus] = {
      runs,
      summary: {
        acceptedRate: calcStats(runs.map((r) => r.acceptedRate)),
        orderLagAt30s: calcStats(runs.map((r) => r.lagAt30s.orderLag)),
        orderCatchupSec: calcStats(runs.map((r) => r.orderCatchupSec)),
        emailLagAt30s: calcStats(runs.map((r) => r.lagAt30s.emailLag)),
        emailCatchupSec: calcStats(runs.map((r) => r.emailCatchupSec)),
        analyticLagAt30s: calcStats(runs.map((r) => r.lagAt30s.analyticLag)),
        analyticCatchupSec: calcStats(runs.map((r) => r.analyticCatchupSec)),
      },
    };
  }

  const outPath = path.join(rawDir, `summary-downstream-${label}.json`);
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2), 'utf8');
  console.log(`\n✅ Saved downstream lag summary to ${outPath}`);
}

main().catch((err) => {
  console.error('\n❌ Downstream lag measurement failed:', err);
  process.exit(1);
});
