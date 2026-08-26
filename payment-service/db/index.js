import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema.js';

// We now use PostgreSQL connection string instead of Supabase REST API
const connectionString = process.env.DATABASE_URL;

const client = postgres(connectionString, { prepare: false });
export const db = drizzle(client, { schema });
