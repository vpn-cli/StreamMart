import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema.js';

const connectionString = process.env.DATABASE_URL;
const poolMax = parseInt(process.env.DB_POOL_MAX || '10', 10);
const client = postgres(connectionString, { max: poolMax, prepare: false });
export const db = drizzle(client, { schema });
