import postgres from 'postgres';
import fs from 'fs';

const envFile = fs.readFileSync('../.env', 'utf8');
const urlLine = envFile.split('\n').find(l => l.startsWith('DATABASE_URL='));
const url = urlLine.split('=')[1].replace(/"/g, '').trim();

const sql = postgres(url, { ssl: 'require' });

async function check() {
  try {
    const file = fs.readFileSync('./drizzle/0000_regular_reptil.sql', 'utf8');
    const queries = file.split('--> statement-breakpoint');
    
    for (const query of queries) {
      if (query.trim()) {
        const safeQuery = query.replace(/CREATE TABLE/g, 'CREATE TABLE IF NOT EXISTS');
        await sql.unsafe(safeQuery);
        console.log("Executed SQL Query block with IF NOT EXISTS");
      }
    }
    
    const tables = await sql`SELECT table_name FROM information_schema.tables WHERE table_schema='public'`;
    console.log("TABLES:", tables.map(t => t.table_name));
    process.exit(0);
  } catch(e) {
    console.error(e);
    process.exit(1);
  }
}
check();
