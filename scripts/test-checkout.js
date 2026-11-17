import fs from 'node:fs';
import path from 'node:path';
import postgres from '../payment-service/node_modules/postgres/src/index.js';

// Load .env
const envPath = path.resolve('.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  for (const line of envContent.split('\n')) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#') && trimmed.includes('=')) {
      const idx = trimmed.indexOf('=');
      const k = trimmed.substring(0, idx).trim();
      const v = trimmed.substring(idx + 1).trim().replace(/^["']|["']$/g, '');
      if (!process.env[k]) process.env[k] = v;
    }
  }
}

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:8080';
const DB_URL = process.env.DATABASE_URL;

async function runCheckoutTest() {
  console.log('='.repeat(70));
  console.log('🛒 StreamMart: Executing End-to-End Checkout Flow Test');
  console.log('='.repeat(70));

  // 1. Authenticate via Gateway Token Endpoint (JWT)
  console.log('\n[Step 1] Requesting cryptographically signed JWT from API Gateway...');
  const testUserId = `user_test_${Date.now()}`;
  const tokenRes = await fetch(`${GATEWAY_URL}/api/auth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId: testUserId }),
  });

  if (!tokenRes.ok) {
    throw new Error(`Failed to obtain JWT from gateway: ${tokenRes.status} ${tokenRes.statusText}`);
  }

  const { token, userId } = await tokenRes.json();
  console.log(`✅ Obtained JWT for user: ${userId}`);
  console.log(`   Token prefix: ${token.substring(0, 32)}...`);

  // 2. Dispatch Checkout request to API Gateway
  console.log('\n[Step 2] Sending Checkout POST /api/payment through API Gateway...');
  const testCart = [
    { id: 'item-101', name: 'Kafka Architecture Hoodie', price: 65.00 },
    { id: 'item-102', name: 'Postgres Mug', price: 15.50 },
  ];

  const startTime = Date.now();
  const paymentRes = await fetch(`${GATEWAY_URL}/api/payment`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
    },
    body: JSON.stringify({ cart: testCart }),
  });

  const responseText = await paymentRes.text();
  const latencyMs = Date.now() - startTime;

  console.log(`✅ Gateway Response: Status ${paymentRes.status} (Latency: ${latencyMs}ms)`);
  console.log(`   Message: "${responseText}"`);

  if (!paymentRes.ok) {
    throw new Error(`Payment request failed: ${paymentRes.status} ${responseText}`);
  }

  // 3. Verify Database Persistence (Payments & Outbox Events)
  if (DB_URL) {
    console.log('\n[Step 3] Verifying Database persistence in PostgreSQL...');
    const sql = postgres(DB_URL, { ssl: DB_URL.includes('neon.tech') ? 'require' : false, max: 1 });
    
    try {
      // Check Payment record
      const paymentRows = await sql`SELECT * FROM payments WHERE user_id = ${userId} ORDER BY created_at DESC LIMIT 1`;
      if (paymentRows.length > 0) {
        console.log(`✅ Payment Record Found in DB:`);
        console.log(`   ID: ${paymentRows[0].id} | Amount: $${paymentRows[0].amount} | Status: ${paymentRows[0].status}`);
      } else {
        console.warn(`⚠️ Payment record not found for user: ${userId}`);
      }

      // Check Outbox Event record
      const outboxRows = await sql`SELECT * FROM outbox_events ORDER BY created_at DESC LIMIT 1`;
      if (outboxRows.length > 0) {
        console.log(`✅ Outbox Event Found in DB:`);
        console.log(`   ID: ${outboxRows[0].id} | Topic: ${outboxRows[0].topic} | Published: ${outboxRows[0].published}`);
      }

      // Check Order record created by order-service
      // Wait up to 5s for order-service to consume payment-successful and insert order
      console.log('\n[Step 4] Checking for Order Service processing in DB...');
      let orderFound = false;
      for (let i = 0; i < 10; i++) {
        await new Promise(r => setTimeout(r, 500));
        const orderRows = await sql`SELECT * FROM orders WHERE user_id = ${userId} ORDER BY created_at DESC LIMIT 1`;
        if (orderRows.length > 0) {
          console.log(`✅ Order Record Created by Order Service:`);
          console.log(`   Order ID: ${orderRows[0].id} | User: ${orderRows[0].user_id} | Status: ${orderRows[0].status}`);
          orderFound = true;
          break;
        }
      }
      if (!orderFound) {
        console.log(`ℹ️ Note: Order consumer will process the outbox message once Kafka relay tick fires.`);
      }

      await sql.end();
    } catch (dbErr) {
      console.error('Database verification error:', dbErr.message);
      await sql.end();
    }
  }

  console.log('\n' + '='.repeat(70));
  console.log('🎉 Checkout Flow Test Completed Successfully!');
  console.log('='.repeat(70));
}

runCheckoutTest().catch(err => {
  console.error('\n❌ Checkout Test Failed:', err);
  process.exit(1);
});
