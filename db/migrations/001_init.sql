CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS raw_events (
  id BIGSERIAL PRIMARY KEY,

  kafka_topic TEXT NOT NULL,
  kafka_partition INTEGER NOT NULL,
  kafka_offset BIGINT NOT NULL,

  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  transaction_id UUID NOT NULL,
  user_id TEXT NOT NULL,
  card_id TEXT NOT NULL,
  merchant_id TEXT NOT NULL,

  amount NUMERIC(12,2) NOT NULL,
  currency TEXT NOT NULL,

  event_time TIMESTAMPTZ NOT NULL,
  ingestion_time TIMESTAMPTZ NOT NULL,

  channel TEXT NOT NULL,
  country TEXT NOT NULL,
  city TEXT NULL,

  device_id TEXT NULL,
  ip_hash TEXT NULL,

  label BOOLEAN NULL,

  payload JSONB NOT NULL,

  CONSTRAINT uq_raw_events_kafka UNIQUE (kafka_topic, kafka_partition, kafka_offset),
  CONSTRAINT uq_raw_events_txn UNIQUE (transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_time ON raw_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_time ON raw_events (user_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_merchant_time ON raw_events (merchant_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_label_time ON raw_events (label, event_time DESC);
