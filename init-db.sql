CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS "orders" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"user_id" text NOT NULL,
	"items" jsonb NOT NULL,
	"status" text NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS "outbox_events" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"topic" text NOT NULL,
	"payload" jsonb NOT NULL,
	"published" boolean DEFAULT false,
	"created_at" timestamp DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS "payments" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"user_id" text NOT NULL,
	"amount" numeric NOT NULL,
	"status" text NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS "processed_events" (
	"event_id" text NOT NULL,
	"service_name" text NOT NULL,
	"processed_at" timestamp DEFAULT now() NOT NULL,
	PRIMARY KEY ("event_id", "service_name")
);

-- High-efficiency partial indexes for transactional outbox relays per topic
CREATE INDEX IF NOT EXISTS idx_outbox_events_payment_unpublished 
  ON outbox_events (created_at ASC) 
  WHERE published = false AND topic = 'payment-successful';

CREATE INDEX IF NOT EXISTS idx_outbox_events_order_unpublished 
  ON outbox_events (created_at ASC) 
  WHERE published = false AND topic = 'order-successful';

CREATE INDEX IF NOT EXISTS idx_outbox_events_email_unpublished 
  ON outbox_events (created_at ASC) 
  WHERE published = false AND topic = 'email-successful';
