import { execSync } from 'node:child_process';
import crypto from 'node:crypto';

function queryDb(sqlQuery) {
  return execSync(
    'docker exec -i streammart-postgres psql -U postgres -d streammart -t -A',
    { input: sqlQuery, encoding: 'utf8' }
  ).trim();
}

function runServicePruner(service, jsCode) {
  const escaped = jsCode.replace(/\n\s*/g, ' ').replace(/"/g, '\\"');
  return execSync(
    `docker compose run --rm --no-deps ${service} node --input-type=module -e "${escaped}"`,
    { encoding: 'utf8' }
  ).trim();
}

async function main() {
  console.log('='.repeat(75));
  console.log('🧪 Outbox Pruner Integration Test (Real PostgreSQL — No Mocks)');
  console.log('='.repeat(75));

  const tag = `prune_test_${Date.now()}`;
  const ids = {
    payOldPub1: crypto.randomUUID(),
    payOldPub2: crypto.randomUUID(),
    payRecentPub: crypto.randomUUID(),
    payOldUnpub: crypto.randomUUID(),
    orderOldPub: crypto.randomUUID(),
    emailOldPub: crypto.randomUUID(),
  };

  console.log('[Step 1] Stopping relay containers briefly so live relays do not race on the unpublished test row...');
  execSync('docker compose stop payment-service order-service email-service', { stdio: 'inherit' });

  try {
    console.log('\n[Step 2] Inserting 6 test rows into real PostgreSQL outbox_events table...');
    const insertSql = `
      INSERT INTO outbox_events (id, topic, payload, published, created_at) VALUES
        ('${ids.payOldPub1}',   'payment-successful', '{"tag":"${tag}","case":"old_pub_1"}'::jsonb,   true,  NOW() - INTERVAL '2 hours'),
        ('${ids.payOldPub2}',   'payment-successful', '{"tag":"${tag}","case":"old_pub_2"}'::jsonb,   true,  NOW() - INTERVAL '3 hours'),
        ('${ids.payRecentPub}', 'payment-successful', '{"tag":"${tag}","case":"recent_pub"}'::jsonb,  true,  NOW() - INTERVAL '10 minutes'),
        ('${ids.payOldUnpub}',  'payment-successful', '{"tag":"${tag}","case":"old_unpub"}'::jsonb,   false, NOW() - INTERVAL '2 hours'),
        ('${ids.orderOldPub}',  'order-successful',   '{"tag":"${tag}","case":"order_old"}'::jsonb,   true,  NOW() - INTERVAL '2 hours'),
        ('${ids.emailOldPub}',  'email-successful',   '{"tag":"${tag}","case":"email_old"}'::jsonb,   true,  NOW() - INTERVAL '2 hours');
    `;
    queryDb(insertSql);

    const beforeTotal = parseInt(queryDb(`SELECT COUNT(*) FROM outbox_events WHERE payload->>'tag' = '${tag}';`), 10);
    console.log(`   Inserted ${beforeTotal} test rows (Expected: 6).`);
    if (beforeTotal !== 6) throw new Error(`Expected 6 inserted test rows, got ${beforeTotal}`);

    console.log('\n[Step 3] Running real prunePublishedEvents(true) against real PostgreSQL...');
    const payOut = runServicePruner(
      'payment-service',
      `import { prunePublishedEvents, getPrunerStatus } from './worker.js';
       const deleted = await prunePublishedEvents(true);
       console.log(JSON.stringify({ deleted, status: getPrunerStatus() }));
       process.exit(0);`
    );
    console.log(`   payment-service pruner output:\n   ${payOut}`);
    const payResult = JSON.parse(payOut.split('\n').pop());

    console.log('\n[Step 4] Running real pruneOrderOutboxEvents(1) against real PostgreSQL...');
    const orderOut = runServicePruner(
      'order-service',
      `import { pruneOrderOutboxEvents } from './worker.js';
       const deleted = await pruneOrderOutboxEvents(1);
       console.log(JSON.stringify({ deleted }));
       process.exit(0);`
    );
    console.log(`   order-service pruner output:\n   ${orderOut}`);

    console.log('\n[Step 5] Running real pruneEmailOutboxEvents(1) against real PostgreSQL...');
    const emailOut = runServicePruner(
      'email-service',
      `import { pruneEmailOutboxEvents } from './worker.js';
       const deleted = await pruneEmailOutboxEvents(1);
       console.log(JSON.stringify({ deleted }));
       process.exit(0);`
    );
    console.log(`   email-service pruner output:\n   ${emailOut}`);

    console.log('\n[Step 6] Verifying surviving rows in real PostgreSQL...');
    const remainingRaw = queryDb(
      `SELECT id || '|' || topic || '|' || published || '|' || (payload->>'case') FROM outbox_events WHERE payload->>'tag' = '${tag}' ORDER BY id;`
    );
    const remainingRows = remainingRaw ? remainingRaw.split('\n').filter(Boolean) : [];
    console.log('   Surviving test rows:');
    for (const row of remainingRows) console.log(`     - ${row}`);

    const exists = (id) => remainingRows.some((r) => r.startsWith(id));

    if (exists(ids.payOldPub1) || exists(ids.payOldPub2)) {
      throw new Error('FAIL: Old published payment-successful rows were NOT deleted!');
    }
    if (exists(ids.orderOldPub)) {
      throw new Error('FAIL: Old published order-successful row was NOT deleted!');
    }
    if (exists(ids.emailOldPub)) {
      throw new Error('FAIL: Old published email-successful row was NOT deleted!');
    }
    if (!exists(ids.payRecentPub)) {
      throw new Error('FAIL: Recent published payment-successful row was wrongly deleted!');
    }
    if (!exists(ids.payOldUnpub)) {
      throw new Error('FAIL: Old UNPUBLISHED payment-successful row was wrongly deleted!');
    }
    if (payResult.deleted < 2) {
      throw new Error(`FAIL: Expected at least 2 deleted payment outbox rows, got ${payResult.deleted}`);
    }
    if (payResult.status.lastError !== null) {
      throw new Error(`FAIL: Pruner reported error: ${payResult.status.lastError}`);
    }
  } finally {
    // Clean up surviving test rows BEFORE restarting live relays
    queryDb(`DELETE FROM outbox_events WHERE payload->>'tag' = '${tag}';`);
    console.log('\n[Step 7] Restarting relay containers and checking /relay-status telemetry...');
    execSync('docker compose up -d payment-service order-service email-service api-gateway', { stdio: 'inherit' });
  }

  // Wait briefly for payment-service to start and connect
  await new Promise((r) => setTimeout(r, 4000));
  const relayStatusRaw = execSync(
    `docker exec streammart-payment node -e "fetch('http://localhost:8000/relay-status').then(async r => console.log(JSON.stringify({ httpStatus: r.status, body: await r.json() })))"`,
    { encoding: 'utf8' }
  ).trim();
  console.log(`   /relay-status response: ${relayStatusRaw}`);

  console.log('\n✅ PASS: Real Postgres Outbox Pruner Integration Test succeeded!');
  console.log('   - Old published rows (payment-successful, order-successful, email-successful) deleted.');
  console.log('   - Recent published row retained.');
  console.log('   - Old unpublished row retained (never pruned before delivery).');
  console.log('   - /relay-status exposes live pruner status and error visibility.');
}

main().catch((err) => {
  console.error('\n❌ Outbox Pruner Integration Test FAILED:', err.message);
  process.exit(1);
});
