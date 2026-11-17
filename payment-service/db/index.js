import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema.js';

// PostgreSQL connection string for Drizzle ORM
const connectionString = process.env.DATABASE_URL;
// Configurable DB connection pool size (defaults to 30 to support high concurrency under peak load)
const poolMax = parseInt(process.env.DB_POOL_MAX || "30", 10);

const client = postgres(connectionString, { max: poolMax, prepare: false });
export const db = drizzle(client, { schema });
