// Latency measurement script: First request vs steady-state (20 sequential checkouts)
import { performance } from "node:perf_hooks";

const GATEWAY_URL = process.env.GATEWAY_URL || "http://localhost:8085";

async function run() {
  console.log(`Connecting to API Gateway at ${GATEWAY_URL}...`);

  // 1. Obtain demo auth token
  const authRes = await fetch(`${GATEWAY_URL}/api/auth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ userId: "latency_benchmark_user" }),
  });

  if (!authRes.ok) {
    throw new Error(`Failed to obtain auth token: ${authRes.status} ${await authRes.text()}`);
  }

  const { token } = await authRes.json();
  console.log("Auth token acquired.");

  const payload = {
    userId: "latency_benchmark_user",
    cart: [
      { id: "item_01", name: "Benchmark Item 1", price: 19.99 },
      { id: "item_02", name: "Benchmark Item 2", price: 29.99 },
    ],
  };

  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${token}`,
  };

  // 2. First checkout request
  console.log("\n--- Executing 1st Checkout Request (Cold Path) ---");
  const t0 = performance.now();
  const firstRes = await fetch(`${GATEWAY_URL}/api/payment`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  const t1 = performance.now();
  const firstLatency = t1 - t0;
  const firstData = await firstRes.text();

  if (!firstRes.ok) {
    throw new Error(`First request failed: ${firstRes.status} ${firstData}`);
  }
  console.log(`First Request Latency: ${firstLatency.toFixed(2)} ms (Status: ${firstRes.status})`);

  // 3. Steady-state checkouts (20 sequential requests)
  console.log("\n--- Executing 20 Sequential Steady-State Checkouts ---");
  const latencies = [];

  for (let i = 1; i <= 20; i++) {
    const start = performance.now();
    const res = await fetch(`${GATEWAY_URL}/api/payment`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    const duration = performance.now() - start;
    latencies.push(duration);
    const text = await res.text();

    if (!res.ok) {
      console.warn(`Request #${i} failed with status ${res.status}: ${text}`);
    } else {
      console.log(`Request #${i.toString().padStart(2, " ")}: ${duration.toFixed(2)} ms`);
    }
  }

  // 4. Statistics calculation
  const sorted = [...latencies].sort((a, b) => a - b);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  const avg = sum / sorted.length;
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)];

  console.log("\n========================================================");
  console.log("             LATENCY BENCHMARK RESULTS                  ");
  console.log("========================================================");
  console.log(`First Request (Cold):     ${firstLatency.toFixed(2)} ms`);
  console.log(`Steady-State Min:         ${min.toFixed(2)} ms`);
  console.log(`Steady-State Max:         ${max.toFixed(2)} ms`);
  console.log(`Steady-State Avg (Mean):  ${avg.toFixed(2)} ms`);
  console.log(`Steady-State p50 (Median):${p50.toFixed(2)} ms`);
  console.log(`Steady-State p95:         ${p95.toFixed(2)} ms`);
  console.log("========================================================\n");
}

run().catch((err) => {
  console.error("Benchmark failed:", err);
  process.exit(1);
});
