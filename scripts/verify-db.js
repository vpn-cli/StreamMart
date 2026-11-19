import fs from 'node:fs';
import postgres from '../payment-service/node_modules/postgres/src/index.js';

const envContent = fs.readFileSync('.env', 'utf8');
const match = envContent.match(/DATABASE_URL=(.+)/);
const dbUrl = match ? match[1].trim().replace(/^["']|["']$/g, '') : null;

const sql = postgres(dbUrl, { ssl: 'require' });

async function check() {
  const payments = await sql`SELECT id, user_id, amount, status, created_at FROM payments ORDER BY created_at DESC LIMIT 5`;
  console.log("Recent Payments:", payments);
  const outbox = await sql`SELECT id, topic, payload, published, created_at FROM outbox_events ORDER BY created_at DESC LIMIT 5`;
  console.log("Recent Outbox Events:", outbox);
  const orders = await sql`SELECT id, user_id, items, status, created_at FROM orders ORDER BY created_at DESC LIMIT 5`;
  console.log("Recent Orders:", orders);
  await sql.end();
}

check().catch(console.error);
