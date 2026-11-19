import { execSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

const k6Binary = 'C:\\Program Files\\k6\\k6.exe';
const resultsFile = path.resolve('benchmarks/raw/experiment-keepalive-dns.json');

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function getTcpStats() {
  try {
    const gwRaw = execSync('docker exec streammart-gateway grep Tcp: /proc/net/snmp', { encoding: 'utf8' });
    const pmRaw = execSync('docker exec streammart-payment grep Tcp: /proc/net/snmp', { encoding: 'utf8' });

    const parseSnmp = (raw) => {
      const lines = raw.trim().split('\n');
      const headers = lines[0].split(/\s+/);
      const values = lines[1].split(/\s+/);
      const activeIdx = headers.indexOf('ActiveOpens');
      const passiveIdx = headers.indexOf('PassiveOpens');
      return {
        activeOpens: parseInt(values[activeIdx], 10),
        passiveOpens: parseInt(values[passiveIdx], 10),
      };
    };

    return {
      gateway: parseSnmp(gwRaw),
      payment: parseSnmp(pmRaw),
    };
  } catch (err) {
    console.error('Error fetching TCP stats:', err.message);
    return null;
  }
}

function getPaymentIp() {
  const ip = execSync(
    'docker inspect streammart-payment --format "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"',
    { encoding: 'utf8' }
  ).trim();
  return ip;
}

async function configureGateway({ keepAlive, targetUrl }) {
  console.log(`\n========================================================================`);
  console.log(`>>> Deploying Gateway: PROXY_KEEP_ALIVE=${keepAlive}, PAYMENT_SERVICE_URL=${targetUrl}`);
  console.log(`========================================================================`);

  execSync('docker compose up -d --build api-gateway', {
    stdio: 'inherit',
    env: {
      ...process.env,
      PROXY_KEEP_ALIVE: String(keepAlive),
      PAYMENT_SERVICE_URL: targetUrl,
    },
  });

  // Wait for health
  for (let i = 0; i < 30; i++) {
    await sleep(1000);
    try {
      const inspect = JSON.parse(execSync('docker inspect streammart-gateway', { encoding: 'utf8' }));
      if (inspect[0]?.State?.Health?.Status === 'healthy') {
        console.log('>>> API Gateway healthy!');
        await sleep(1000);
        return;
      }
    } catch (_) {}
  }
  throw new Error('API Gateway failed to become healthy');
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
      const count = summary.metrics.http_reqs?.count || 0;
      const rate = summary.metrics.http_reqs?.rate || 0;
      const p50 = summary.metrics.http_req_duration?.med || 0;
      const p95 = summary.metrics.http_req_duration?.['p(95)'] || 0;
      const failedRate = (summary.metrics.http_req_failed?.value || 0) * 100;

      resolve({ count, rate, p50, p95, errorRate: failedRate });
    });
  });
}

async function runScenario(scenarioName, config, vus = 20, duration = '30s') {
  await configureGateway(config);

  const summaryFile = path.resolve(`benchmarks/raw/exp-${scenarioName}-summary.json`);
  const rawFile = path.resolve(`benchmarks/raw/exp-${scenarioName}-raw.txt`);

  // Warmup run
  console.log(`>>> [Warmup] 10s run...`);
  await runK6(vus, '10s', path.resolve(`benchmarks/raw/exp-${scenarioName}-warmup.json`), path.resolve(`benchmarks/raw/exp-${scenarioName}-warmup.txt`));
  await sleep(2000);

  // Measure TCP stats BEFORE
  const statsBefore = getTcpStats();
  const startTime = performance.now();

  console.log(`\n>>> [Measured Run] ${scenarioName} @ ${vus} VUs for ${duration}...`);
  const k6Res = await runK6(vus, duration, summaryFile, rawFile);
  const elapsedSec = (performance.now() - startTime) / 1000;

  // Measure TCP stats AFTER
  const statsAfter = getTcpStats();

  const deltaActive = statsAfter.gateway.activeOpens - statsBefore.gateway.activeOpens;
  const deltaPassive = statsAfter.payment.passiveOpens - statsBefore.payment.passiveOpens;
  const newConnectionsSec = deltaActive / elapsedSec;

  const result = {
    scenario: scenarioName,
    config,
    vus,
    durationSec: Number(elapsedSec.toFixed(2)),
    acceptedReqs: k6Res.count,
    acceptedRate: Number(k6Res.rate.toFixed(2)),
    p50: Number(k6Res.p50.toFixed(2)),
    p95: Number(k6Res.p95.toFixed(2)),
    gatewayActiveOpens: deltaActive,
    paymentPassiveOpens: deltaPassive,
    newConnectionsPerSec: Number(newConnectionsSec.toFixed(2)),
  };

  console.log('\n--- Scenario Results ---');
  console.log(`Scenario: ${scenarioName}`);
  console.log(`Accepted Throughput: ${result.acceptedRate} req/s`);
  console.log(`p50 Latency: ${result.p50} ms | p95: ${result.p95} ms`);
  console.log(`Gateway New Outbound Connections: ${deltaActive} (${result.newConnectionsPerSec} conn/s)`);
  console.log(`Payment New Inbound Connections: ${deltaPassive}`);

  return result;
}

async function main() {
  const paymentIp = getPaymentIp();
  console.log(`Detected Payment Service Container IP: ${paymentIp}`);

  const scenarios = [
    {
      name: 'keepalive_off_hostname',
      config: { keepAlive: false, targetUrl: 'http://payment-service:8000' },
    },
    {
      name: 'keepalive_off_container_ip',
      config: { keepAlive: false, targetUrl: `http://${paymentIp}:8000` },
    },
    {
      name: 'keepalive_on_hostname',
      config: { keepAlive: true, targetUrl: 'http://payment-service:8000' },
    },
  ];

  const allResults = [];
  for (const s of scenarios) {
    const res = await runScenario(s.name, s.config, 20, '30s');
    allResults.push(res);
    await sleep(2000);
  }

  fs.writeFileSync(resultsFile, JSON.stringify(allResults, null, 2), 'utf8');
  console.log(`\n========================================================================`);
  console.log(`✅ All experiment scenarios finished. Results saved to ${resultsFile}`);
  console.log(`========================================================================`);
  console.table(allResults);
}

main().catch((err) => {
  console.error('Experiment failed:', err);
  process.exit(1);
});
