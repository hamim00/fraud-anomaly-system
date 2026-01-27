"""
DATABASE MODULE - Handles saving features to PostgreSQL
================================================================================

This module provides database operations for the feature consumer:
1. Connect to PostgreSQL with retry logic
2. Insert feature rows (with UPSERT to handle duplicates)

WHY UPSERT?
-----------
If we process the same transaction twice (due to Kafka rebalance, restart, etc.),
we don't want to crash or create duplicates. UPSERT means:
- If transaction_id doesn't exist: INSERT new row
- If transaction_id exists: UPDATE the existing row

This makes our system IDEMPOTENT (safe to run multiple times).

================================================================================
"""

import os
import time
import logging
from typing import Any, Dict

import psycopg2
import psycopg2.extras

log = logging.getLogger("feature_consumer.db")


def get_pg_dsn() -> str:
    """Build PostgreSQL connection string from environment variables."""
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    db = os.getenv("PGDATABASE", "fraud_db")
    user = os.getenv("PGUSER", "fraud")
    pwd = os.getenv("PGPASSWORD", "fraud")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def connect_with_retry(max_attempts: int = 60, sleep_s: float = 1.0):
    """
    Connect to PostgreSQL, retrying if database isn't ready yet.
    
    When starting with Docker Compose, the database might not be ready
    immediately. This function keeps trying for up to 60 seconds.
    
    Args:
        max_attempts: Maximum number of connection attempts
        sleep_s: Seconds to wait between attempts
    
    Returns:
        psycopg2 connection object
    
    Raises:
        RuntimeError if connection fails after all attempts
    """
    dsn = get_pg_dsn()
    
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False  # We'll manage transactions manually
            log.info("Connected to PostgreSQL")
            return conn
        except Exception as e:
            log.warning("PostgreSQL connection attempt %d/%d failed: %s", 
                       attempt, max_attempts, e)
            time.sleep(sleep_s)
    
    raise RuntimeError("Failed to connect to PostgreSQL after retries")


# =============================================================================
# UPSERT SQL - Insert or Update features
# =============================================================================
# 
# ON CONFLICT (transaction_id) DO UPDATE:
# - If transaction_id already exists, update all fields
# - This ensures idempotency (safe to process same transaction twice)
# =============================================================================

UPSERT_FEATURES_SQL = """
INSERT INTO transaction_features (
    transaction_id,
    user_id,
    event_time,
    
    amount,
    amount_zscore,
    
    user_txn_count_1h,
    user_txn_count_24h,
    user_txn_count_7d,
    user_amount_sum_1h,
    user_amount_sum_24h,
    user_avg_amount_30d,
    user_std_amount_30d,
    
    country_change_flag,
    device_change_flag,
    unique_countries_24h,
    unique_merchants_24h,
    unique_devices_24h,
    user_merchant_first_time,
    
    hour_of_day,
    day_of_week,
    is_weekend,
    is_night,
    minutes_since_last_txn,
    
    channel,
    channel_encoded,
    
    country,
    is_foreign_txn,
    
    label
)
VALUES (
    %(transaction_id)s,
    %(user_id)s,
    %(event_time)s,
    
    %(amount)s,
    %(amount_zscore)s,
    
    %(user_txn_count_1h)s,
    %(user_txn_count_24h)s,
    %(user_txn_count_7d)s,
    %(user_amount_sum_1h)s,
    %(user_amount_sum_24h)s,
    %(user_avg_amount_30d)s,
    %(user_std_amount_30d)s,
    
    %(country_change_flag)s,
    %(device_change_flag)s,
    %(unique_countries_24h)s,
    %(unique_merchants_24h)s,
    %(unique_devices_24h)s,
    %(user_merchant_first_time)s,
    
    %(hour_of_day)s,
    %(day_of_week)s,
    %(is_weekend)s,
    %(is_night)s,
    %(minutes_since_last_txn)s,
    
    %(channel)s,
    %(channel_encoded)s,
    
    %(country)s,
    %(is_foreign_txn)s,
    
    %(label)s
)
ON CONFLICT (transaction_id) DO UPDATE SET
    amount_zscore = EXCLUDED.amount_zscore,
    user_txn_count_1h = EXCLUDED.user_txn_count_1h,
    user_txn_count_24h = EXCLUDED.user_txn_count_24h,
    user_txn_count_7d = EXCLUDED.user_txn_count_7d,
    user_amount_sum_1h = EXCLUDED.user_amount_sum_1h,
    user_amount_sum_24h = EXCLUDED.user_amount_sum_24h,
    user_avg_amount_30d = EXCLUDED.user_avg_amount_30d,
    user_std_amount_30d = EXCLUDED.user_std_amount_30d,
    country_change_flag = EXCLUDED.country_change_flag,
    device_change_flag = EXCLUDED.device_change_flag,
    unique_countries_24h = EXCLUDED.unique_countries_24h,
    unique_merchants_24h = EXCLUDED.unique_merchants_24h,
    unique_devices_24h = EXCLUDED.unique_devices_24h,
    user_merchant_first_time = EXCLUDED.user_merchant_first_time,
    minutes_since_last_txn = EXCLUDED.minutes_since_last_txn,
    computed_at = now();
"""


def upsert_features(conn, features: Dict[str, Any]) -> None:
    """
    Insert or update a feature row in the database.
    
    Args:
        conn: PostgreSQL connection
        features: Dictionary of feature values from FeatureCalculator
    """
    with conn.cursor() as cur:
        cur.execute(UPSERT_FEATURES_SQL, features)


def ensure_features_table_exists(conn) -> None:
    """
    Check if transaction_features table exists.
    If not, log a warning (user needs to run migration).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'transaction_features'
            );
        """)
        exists = cur.fetchone()[0]
        
        if not exists:
            log.error(
                "Table 'transaction_features' does not exist! "
                "Please run the migration: 002_transaction_features.sql"
            )
            raise RuntimeError("Missing transaction_features table")
        
        log.info("Table 'transaction_features' exists")


def get_feature_count(conn) -> int:
    """Get the current count of rows in transaction_features table."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM transaction_features")
        return cur.fetchone()[0]
