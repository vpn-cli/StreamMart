import { execSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

const k6Binary = 'C:\\Program Files\\k6\\k6.exe';
const rawDir = path.resolve('benchmarks/raw');
if (!fs.existsSync(rawDir)) fs.mkdirSync(rawDir, { recursive: true });

const targetConfig = process.argv[2] || 'all';

const CONFIGS = {
  baseline: { poolMax: 10, keepAlive: false },
  pool_only: { poolMax: 30, keepAlive: false },
  keepalive_only: { poolMax: 10, keepAlive: true },
  pool_keepalive: { poolMax: 30, keepAlive: true },
  pool_keepalive_optimized: { poolMax: 30, keepAlive: true },
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runCommand(cmd, env = {}) {
  return execSync(cmd, {
    stdio: 'inherit',
    env: { ...process.env, ...env },
  });
}

async function setConfiguration(configName, { poolMax, keepAlive }) {
  console.log(`\n========================================================================`);
  console.log(`>>> [CONFIG] Deploying: ${configName} (DB_POOL_MAX=${poolMax}, PROXY_KEEP_ALIVE=${keepAlive})`);
  console.log(`========================================================================`);

  runCommand('docker compose up -d --build api-gateway payment-service', {
    DB_POOL_MAX: String(poolMax),
    PROXY_KEEP_ALIVE: String(keepAlive),
    PAYMENT_SERVICE_URL: 'http://payment-service:8000',
  });

  console.log('>>> Waiting for api-gateway and payment-service healthchecks...');
  for (let i = 0; i < 30; i++) {
    try {
      const gw = execSync('docker inspect streammart-gateway --format "{{.State.Health.Status}}"', { encoding: 'utf8' }).replace(/['"\s]/g, '');
      const pm = execSync('docker inspect streammart-payment --format "{{.State.Health.Status}}"', { encoding: 'utf8' }).replace(/['"\s]/g, '');
      if (gw === 'healthy' && pm === 'healthy') {
        console.log('>>> Containers healthy!');
        await sleep(2000);
        return;
      }
    } catch (_) {}
    await sleep(2000);
  }
  throw new Error('Containers failed to become healthy within 60s');
}

async function drainOutbox() {
  function getBacklog() {
    try {
      const out = execSync(
        'docker exec streammart-postgres psql -U postgres -d streammart -t -A -c "SELECT count(*) FROM outbox_events WHERE published = false AND topic = \'payment-successful\';"',
        { encoding: 'utf8' }
      ).trim();
      return parseInt(out || '0', 10);
    } catch (_) {
      return 0;
    }
  }

  const backlog = getBacklog();
  if (backlog === 0) {
    return { backlog: 0, drainSec: 0 };
  }

  console.log(`    [Drain] Backlog: ${backlog} unpublished events. Waiting for relay to drain...`);
  const start = performance.now();
  let remaining = backlog;
  const timeout = performance.now() + 60000;

  while (remaining > 0 && performance.now() < timeout) {
    await sleep(250);
    remaining = getBacklog();
  }

  const drainSec = Number(((performance.now() - start) / 1000).toFixed(2));
  console.log(`    [Drain] Done in ${drainSec}s (${(backlog / (drainSec || 0.001)).toFixed(1)} events/s).`);
  return { backlog, drainSec };
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
      const rps = summary.metrics.http_reqs?.rate || 0;
      const p50 = summary.metrics.http_req_duration?.med || 0;
      const p95 = summary.metrics.http_req_duration?.['p(95)'] || 0;
      const failedRate = (summary.metrics.http_req_failed?.value || 0) * 100;

      resolve({ rps, p50, p95, errorRate: failedRate });
    });
  });
}

function calcStats(numbers) {
  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const mean = numbers.reduce((a, b) => a + b, 0) / numbers.length;
  return { mean, min, max };
}

async function runBenchmarkConfig(configName, configSettings) {
  await setConfiguration(configName, configSettings);

  const configResults = {};

  for (const vus of [20, 50, 100]) {
    console.log(`\n------------------------------------------------------------------------`);
    console.log(`🚦 Tier: ${configName} @ ${vus} VUs (1 Warm-up + 3 Measured Runs)`);
    console.log(`------------------------------------------------------------------------`);

    // 1. Warm-up (discarded)
    console.log(`>>> [Warm-up] ${vus} VUs for 30s (discarded)...`);
    const warmupSummary = path.join(rawDir, `${configName}-${vus}vu-warmup.json`);
    const warmupRaw = path.join(rawDir, `${configName}-${vus}vu-warmup.txt`);
    await runK6(vus, '30s', warmupSummary, warmupRaw);
    await drainOutbox();
    await sleep(2000);

    // 2. Three measured runs
    const measuredRuns = [];
    for (let r = 1; r <= 3; r++) {
      console.log(`\n>>> [Measured Run ${r}/3] ${configName} @ ${vus} VUs for 30s...`);
      const summaryFile = path.join(rawDir, `${configName}-${vus}vu-run${r}.json`);
      const rawFile = path.join(rawDir, `${configName}-${vus}vu-run${r}.txt`);

      const k6Metrics = await runK6(vus, '30s', summaryFile, rawFile);
      const drain = await drainOutbox();

      measuredRuns.push({
        run: r,
        rps: k6Metrics.rps,
        p50: k6Metrics.p50,
        p95: k6Metrics.p95,
        errorRate: k6Metrics.errorRate,
        backlog: drain.backlog,
        drainSec: drain.drainSec,
      });

      await sleep(2000);
    }

    const rpsStats = calcStats(measuredRuns.map((r) => r.rps));
    const p50Stats = calcStats(measuredRuns.map((r) => r.p50));
    const p95Stats = calcStats(measuredRuns.map((r) => r.p95));
    const errStats = calcStats(measuredRuns.map((r) => r.errorRate));
    const backlogStats = calcStats(measuredRuns.map((r) => r.backlog));
    const drainStats = calcStats(measuredRuns.map((r) => r.drainSec));

    configResults[vus] = {
      runs: measuredRuns,
      summary: {
        rps: rpsStats,
        p50: p50Stats,
        p95: p95Stats,
        errorRate: errStats,
        backlog: backlogStats,
        drainSec: drainStats,
      },
    };
  }

  const outJson = path.join(rawDir, `summary-${configName}.json`);
  fs.writeFileSync(outJson, JSON.stringify(configResults, null, 2), 'utf8');
  console.log(`\n✅ Finished ${configName}. Summary saved to ${outJson}`);
  return configResults;
}

async function main() {
  const configsToRun = targetConfig === 'all'
    ? Object.keys(CONFIGS)
    : [targetConfig];

  for (const name of configsToRun) {
    if (!CONFIGS[name]) throw new Error(`Unknown config: ${name}`);
    await runBenchmarkConfig(name, CONFIGS[name]);
  }
}

main().catch((err) => {
  console.error('\n❌ Benchmark runner error:', err);
  process.exit(1);
});
