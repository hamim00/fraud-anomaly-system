"""
DRIFT DETECTOR - Reference Data Loader
=======================================

Loads reference (training) data and current data from PostgreSQL
for drift comparison.

WHAT IS REFERENCE DATA?
-----------------------
Reference data is a snapshot of what "normal" looks like.
Usually this is the data the model was trained on.

We compare CURRENT data against REFERENCE data to detect drift.

Example:
- Reference: First 5000 transactions (what model learned)
- Current: Last 1000 transactions (what's happening now)
- If they're very different → DRIFT DETECTED!
"""

import logging
from typing import Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras

from config import config

log = logging.getLogger("drift_detector.data")


def get_connection():
    """Create a database connection."""
    return psycopg2.connect(
        host=config.db.host,
        port=config.db.port,
        database=config.db.database,
        user=config.db.user,
        password=config.db.password,
    )


def load_reference_data(limit: int = None) -> Optional[pd.DataFrame]:
    """
    Load REFERENCE data (oldest N rows from transaction_features).
    
    This represents the "training distribution" - what the model
    considers normal.
    
    Args:
        limit: Maximum number of rows (default from config)
    
    Returns:
        DataFrame with features, or None if not enough data
    """
    if limit is None:
        limit = config.drift.reference_window_size
    
    query = """
    SELECT 
        transaction_id,
        amount,
        amount_zscore,
        user_avg_amount_30d,
        user_txn_count_1h,
        user_txn_count_24h,
        user_txn_count_7d,
        user_amount_sum_1h,
        user_amount_sum_24h,
        country_change_flag,
        device_change_flag,
        unique_countries_24h,
        unique_merchants_24h,
        user_merchant_first_time,
        hour_of_day,
        day_of_week,
        is_weekend,
        is_night,
        minutes_since_last_txn,
        channel_encoded,
        label,
        created_at
    FROM transaction_features
    ORDER BY created_at ASC
    LIMIT %s
    """
    
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=(limit,))
        conn.close()
        
        if len(df) < config.drift.min_samples:
            log.warning(
                f"Not enough reference data: {len(df)} rows "
                f"(need {config.drift.min_samples})"
            )
            return None
        
        log.info(f"Loaded {len(df)} reference rows")
        return df
        
    except Exception as e:
        log.exception(f"Failed to load reference data: {e}")
        return None


def load_current_data(limit: int = None) -> Optional[pd.DataFrame]:
    """
    Load CURRENT data (newest N rows from transaction_features).
    
    This represents the "current distribution" - what's happening
    right now in production.
    
    Args:
        limit: Maximum number of rows (default from config)
    
    Returns:
        DataFrame with features, or None if not enough data
    """
    if limit is None:
        limit = config.drift.current_window_size
    
    query = """
    SELECT 
        transaction_id,
        amount,
        amount_zscore,
        user_avg_amount_30d,
        user_txn_count_1h,
        user_txn_count_24h,
        user_txn_count_7d,
        user_amount_sum_1h,
        user_amount_sum_24h,
        country_change_flag,
        device_change_flag,
        unique_countries_24h,
        unique_merchants_24h,
        user_merchant_first_time,
        hour_of_day,
        day_of_week,
        is_weekend,
        is_night,
        minutes_since_last_txn,
        channel_encoded,
        label,
        created_at
    FROM transaction_features
    ORDER BY created_at DESC
    LIMIT %s
    """
    
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=(limit,))
        conn.close()
        
        if len(df) < config.drift.min_samples:
            log.warning(
                f"Not enough current data: {len(df)} rows "
                f"(need {config.drift.min_samples})"
            )
            return None
        
        log.info(f"Loaded {len(df)} current rows")
        return df
        
    except Exception as e:
        log.exception(f"Failed to load current data: {e}")
        return None


def load_reference_and_current() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Load both reference and current data.
    
    Returns:
        Tuple of (reference_df, current_df), either may be None
    """
    reference = load_reference_data()
    current = load_current_data()
    return reference, current


def get_data_stats() -> dict:
    """
    Get statistics about available data.
    
    Returns:
        Dictionary with counts and date ranges
    """
    query = """
    SELECT 
        COUNT(*) as total_rows,
        MIN(created_at) as oldest,
        MAX(created_at) as newest,
        COUNT(DISTINCT DATE(created_at)) as distinct_days
    FROM transaction_features
    """
    
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()
        conn.close()
        
        return {
            "total_rows": result["total_rows"],
            "oldest": result["oldest"].isoformat() if result["oldest"] else None,
            "newest": result["newest"].isoformat() if result["newest"] else None,
            "distinct_days": result["distinct_days"],
            "reference_size": config.drift.reference_window_size,
            "current_size": config.drift.current_window_size,
            "min_samples": config.drift.min_samples,
        }
        
    except Exception as e:
        log.exception(f"Failed to get data stats: {e}")
        return {"error": str(e)}
