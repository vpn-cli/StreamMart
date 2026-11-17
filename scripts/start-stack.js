import { spawn } from 'node:child_process';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';

// Load .env
const envPath = path.resolve('.env');
const env = { ...process.env };
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  for (const line of envContent.split('\n')) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#') && trimmed.includes('=')) {
      const idx = trimmed.indexOf('=');
      const k = trimmed.substring(0, idx).trim();
      const v = trimmed.substring(idx + 1).trim().replace(/^["']|["']$/g, '');
      if (!env[k]) env[k] = v;
    }
  }
}

// Default environment settings
env.PORT = env.PORT || '8080';
env.PAYMENT_SERVICE_URL = env.PAYMENT_SERVICE_URL || 'http://localhost:8000';
env.JWT_SECRET = env.JWT_SECRET || 'streammart-super-secure-jwt-secret-key-32-bytes!';
env.KAFKA_BROKERS = env.KAFKA_BROKERS || 'localhost:9092';

const gatewayPort = Number(env.PORT) || 8085;

const services = [
  { name: 'API-GATEWAY', dir: './api-gateway', file: 'index.js', port: gatewayPort, health: '/health' },
  { name: 'PAYMENT-SVC', dir: './payment-service', file: 'index.js', port: 8000, health: '/health' },
  { name: 'ORDER-SVC', dir: './order-service', file: 'index.js', port: 8001, health: '/health' },
  { name: 'EMAIL-SVC', dir: './email-service', file: 'index.js', port: 8002, health: '/health' },
  { name: 'ANALYTIC-SVC', dir: './analytic-service', file: 'index.js', port: 8003, health: '/health' },
];

const children = [];

function checkHealth(port, path) {
  return new Promise((resolve) => {
    const req = http.get(`http://localhost:${port}${path}`, (res) => {
      resolve(res.statusCode === 200);
    });
    req.on('error', () => resolve(false));
    req.setTimeout(1000, () => {
      req.destroy();
      resolve(false);
    });
  });
}

async function waitForHealth(service, maxRetries = 30) {
  for (let i = 0; i < maxRetries; i++) {
    const healthy = await checkHealth(service.port, service.health);
    if (healthy) return true;
    await new Promise(r => setTimeout(r, 500));
  }
  return false;
}

console.log('='.repeat(70));
console.log('🚀 StreamMart: Starting Microservices Stack (One-Command Startup)');
console.log('='.repeat(70));

for (const svc of services) {
  const child = spawn('node', [svc.file], {
    cwd: path.resolve(svc.dir),
    env: { ...env, PORT: String(svc.port) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (d) => {
    for (const line of d.toString().split('\n')) {
      if (line.trim()) console.log(`[${svc.name}] ${line.trim()}`);
    }
  });

  child.stderr.on('data', (d) => {
    for (const line of d.toString().split('\n')) {
      if (line.trim()) console.error(`[${svc.name}] [ERR] ${line.trim()}`);
    }
  });

  child.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      console.warn(`[${svc.name}] Exited with code ${code}`);
    }
  });

  children.push({ ...svc, process: child });
}

// Health check barrier
console.log('\n⏳ Waiting for all services to report healthy on their /health probes...');
let allHealthy = true;

for (const svc of services) {
  const ok = await waitForHealth(svc);
  if (ok) {
    console.log(`✅ [${svc.name}] is HEALTHY on port ${svc.port}`);
  } else {
    console.error(`❌ [${svc.name}] FAILED health check on port ${svc.port}`);
    allHealthy = false;
  }
}

if (allHealthy) {
  console.log('\n' + '='.repeat(70));
  console.log('🎉 All 5 StreamMart microservices are ONLINE and HEALTHY!');
  console.log('   - API Gateway:    http://localhost:8080/health');
  console.log('   - Payment Svc:    http://localhost:8000/health');
  console.log('   - Order Svc:      http://localhost:8001/health');
  console.log('   - Email Svc:      http://localhost:8002/health');
  console.log('   - Analytic Svc:   http://localhost:8003/health');
  console.log('='.repeat(70));
} else {
  console.error('\n⚠️ Some services failed to report healthy within timeout.');
}

const shutdown = () => {
  console.log('\n🛑 Shutting down microservices stack...');
  for (const c of children) {
    try {
      c.process.kill('SIGTERM');
    } catch {}
  }
  setTimeout(() => process.exit(0), 1000);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
