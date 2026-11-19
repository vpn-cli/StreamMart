import http from 'http';

const concurrency = parseInt(process.argv[2] || '50', 10);
const durationMs = 10000;
const payload = JSON.stringify({
  cart: [
    { id: 'k6_item_1', name: 'Cloud Native T-Shirt', price: 29.99 },
    { id: 'k6_item_2', name: 'Distributed Systems Cap', price: 19.50 },
  ],
});

let totalRequests = 0;
let errors = 0;
const latencies = [];
let keepRunning = true;

const agent = new http.Agent({ keepAlive: true, maxSockets: concurrency });

function makeRequest() {
  return new Promise((resolve) => {
    const start = performance.now();
    const req = http.request({
      host: 'payment-service',
      port: 8000,
      path: '/payment',
      method: 'POST',
      agent,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
        'x-user-id': 'bench_direct_user',
      },
    }, (res) => {
      res.on('data', () => {});
      res.on('end', () => {
        const dur = performance.now() - start;
        latencies.push(dur);
        totalRequests++;
        if (res.statusCode !== 200) errors++;
        resolve();
      });
    });
    req.on('error', () => {
      errors++;
      resolve();
    });
    req.write(payload);
    req.end();
  });
}

async function worker() {
  while (keepRunning) {
    await makeRequest();
    await new Promise((r) => setTimeout(r, 50));
  }
}

async function main() {
  console.log(`Starting direct payment-service load test (${concurrency} workers, ${durationMs/1000}s)...`);
  setTimeout(() => { keepRunning = false; }, durationMs);
  const start = performance.now();
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  const elapsed = (performance.now() - start) / 1000;

  latencies.sort((a,b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.5)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  const rps = totalRequests / elapsed;

  console.log(`Direct Payment Service Results (${concurrency} workers):`);
  console.log(`Throughput: ${rps.toFixed(2)} req/s`);
  console.log(`Total Requests: ${totalRequests}, Errors: ${errors}`);
  console.log(`p50: ${p50?.toFixed(2)} ms, p95: ${p95?.toFixed(2)} ms`);
}

main().catch(console.error);