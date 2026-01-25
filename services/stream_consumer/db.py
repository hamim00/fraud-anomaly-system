import os
import time
import logging
from typing import Any, Dict

import psycopg2


def get_pg_dsn() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    db = os.getenv("PGDATABASE", "fraud_db")
    user = os.getenv("PGUSER", "fraud")
    pwd = os.getenv("PGPASSWORD", "fraud")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def connect_with_retry(max_attempts: int = 60, sleep_s: float = 1.0):
    log = logging.getLogger("stream_consumer.db")
    dsn = get_pg_dsn()

    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            log.info("Connected to Postgres.")
            return conn
        except Exception as e:
            log.warning("Postgres connect attempt %d/%d failed: %s", attempt, max_attempts, e)
            time.sleep(sleep_s)

    raise RuntimeError("Failed to connect to Postgres after retries.")


INSERT_SQL = """
INSERT INTO raw_events (
  kafka_topic, kafka_partition, kafka_offset,
  transaction_id, user_id, card_id, merchant_id,
  amount, currency,
  event_time, ingestion_time,
  channel, country, city,
  device_id, ip_hash,
  label,
  payload
)
VALUES (
  %(kafka_topic)s, %(kafka_partition)s, %(kafka_offset)s,
  %(transaction_id)s, %(user_id)s, %(card_id)s, %(merchant_id)s,
  %(amount)s, %(currency)s,
  %(event_time)s, %(ingestion_time)s,
  %(channel)s, %(country)s, %(city)s,
  %(device_id)s, %(ip_hash)s,
  %(label)s,
  %(payload)s
)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING;
"""


def insert_raw_event(conn, row: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, row)
