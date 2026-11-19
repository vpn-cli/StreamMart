import { execSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const k6Binary = 'C:\\Program Files\\k6\\k6.exe';

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

async function runTest() {
  console.log('Testing ORIGINAL master gateway behavior (no custom agent passed to http-proxy-middleware)...');
  
  // Backup current api-gateway/index.js
  const currentCode = fs.readFileSync('api-gateway/index.js', 'utf8');
  
  // Create master-like index.js without custom agent
  const masterLikeCode = currentCode.replace(
    'agent: paymentProxyHttpAgent,',
    '// agent omitted (master behavior)'
  );
  
  fs.writeFileSync('api-gateway/index.js', masterLikeCode, 'utf8');
  
  try {
    execSync('docker compose up -d --build api-gateway', { stdio: 'inherit' });
    
    // Wait for healthy
    for (let i = 0; i < 30; i++) {
      await sleep(1000);
      try {
        const inspect = JSON.parse(execSync('docker inspect streammart-gateway', { encoding: 'utf8' }));
        if (inspect[0]?.State?.Health?.Status === 'healthy') break;
      } catch (_) {}
    }
    
    console.log('Gateway healthy. Running 20 VU test for 30s...');
    
    const tcpBefore = getTcpStats();
    const t0 = performance.now();
    
    execSync(`"${k6Binary}" run --vus 20 --duration 30s benchmarks/gateway-load.js`, { stdio: 'inherit' });
    
    const durationSec = (performance.now() - t0) / 1000;
    const tcpAfter = getTcpStats();
    
    const gwActive = tcpAfter.gateway.activeOpens - tcpBefore.gateway.activeOpens;
    const pmPassive = tcpAfter.payment.passiveOpens - tcpBefore.payment.passiveOpens;
    
    console.log('\n======================================================');
    console.log('ORIGINAL MASTER CODE MEASUREMENT RESULTS:');
    console.log('Duration:', durationSec.toFixed(2), 's');
    console.log('Gateway ActiveOpens (outbound TCP opens):', gwActive);
    console.log('Payment PassiveOpens (inbound TCP opens):', pmPassive);
    console.log('Outbound TCP opens / sec:', (gwActive / durationSec).toFixed(2));
    console.log('======================================================\n');
  } finally {
    // Restore current code
    fs.writeFileSync('api-gateway/index.js', currentCode, 'utf8');
  }
}

runTest().catch(console.error);
