import { pgTable, uuid, text, timestamp, jsonb, primaryKey } from 'drizzle-orm/pg-core';

export const orders = pgTable('orders', {
  id: uuid('id').defaultRandom().primaryKey(),
  userId: text('user_id').notNull(),
  items: jsonb('items').notNull(),
  status: text('status').notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const processedEvents = pgTable('processed_events', {
  eventId: text('event_id').notNull(),
  serviceName: text('service_name').notNull(),
  processedAt: timestamp('processed_at').defaultNow().notNull(),
}, (table) => [
  primaryKey({ columns: [table.eventId, table.serviceName] })
]);
