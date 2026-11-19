import { pgTable, text, timestamp, primaryKey } from 'drizzle-orm/pg-core';

export const processedEvents = pgTable('processed_events', {
  eventId: text('event_id').notNull(),
  serviceName: text('service_name').notNull(),
  processedAt: timestamp('processed_at').defaultNow().notNull(),
}, (table) => [
  primaryKey({ columns: [table.eventId, table.serviceName] })
]);
