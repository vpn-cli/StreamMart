import { pgTable, uuid, text, timestamp, jsonb, boolean, primaryKey } from 'drizzle-orm/pg-core';

export const outboxEvents = pgTable('outbox_events', {
  id: uuid('id').defaultRandom().primaryKey(),
  topic: text('topic').notNull(),
  payload: jsonb('payload').notNull(),
  published: boolean('published').default(false),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const processedEvents = pgTable('processed_events', {
  eventId: text('event_id').notNull(),
  serviceName: text('service_name').notNull(),
  processedAt: timestamp('processed_at').defaultNow().notNull(),
}, (table) => [
  primaryKey({ columns: [table.eventId, table.serviceName] })
]);
