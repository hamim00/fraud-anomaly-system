"""
DRIFT DETECTOR - Reference Data Loader (FIXED v3)
=================================================

Loads reference (training) data and current data from PostgreSQL
for drift comparison.

FIXES:
- v1: Removed created_at column, use transaction_id for ordering
- v2: Convert Decimal to float for pandas compatibility
- v3: Fixed Pylance type warnings
"""

import logging
from typing import Optional, Tuple, Dict, Any
from decimal import Decimal

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


def convert_decimals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all Decimal columns to float.
    
    PostgreSQL returns NUMERIC/DECIMAL as Python Decimal objects,
    but pandas/numpy need float for calculations.
    """
    for col in df.columns:
        if df[col].dtype == object:
            # Check if first non-null value is Decimal
            non_null = df[col].dropna()
            if len(non_null) > 0:
                first_val = non_null.iloc[0]
                if isinstance(first_val, Decimal):
                    df[col] = df[col].astype(float)
    return df


def ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all columns except transaction_id are numeric.
    """
    for col in df.columns:
        if col != 'transaction_id':
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            except Exception:
                pass
    return df


def load_reference_data(limit: Optional[int] = None) -> Optional[pd.DataFrame]:
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
    
    # Query only the features we need for drift detection
    # Use transaction_id for ordering (it's sequential)
    # Cast NUMERIC to DOUBLE PRECISION to get float instead of Decimal
    query = """
    SELECT 
        transaction_id,
        amount::double precision as amount,
        COALESCE(amount_zscore, 0)::double precision as amount_zscore,
        COALESCE(user_avg_amount_30d, 0)::double precision as user_avg_amount_30d,
        COALESCE(user_txn_count_1h, 0)::double precision as user_txn_count_1h,
        COALESCE(user_txn_count_24h, 0)::double precision as user_txn_count_24h,
        COALESCE(user_txn_count_7d, 0)::double precision as user_txn_count_7d,
        COALESCE(user_amount_sum_1h, 0)::double precision as user_amount_sum_1h,
        COALESCE(user_amount_sum_24h, 0)::double precision as user_amount_sum_24h,
        COALESCE(country_change_flag, false)::int as country_change_flag,
        COALESCE(device_change_flag, false)::int as device_change_flag,
        COALESCE(unique_countries_24h, 1)::double precision as unique_countries_24h,
        COALESCE(unique_merchants_24h, 1)::double precision as unique_merchants_24h,
        COALESCE(user_merchant_first_time, false)::int as user_merchant_first_time,
        COALESCE(hour_of_day, 12)::double precision as hour_of_day,
        COALESCE(day_of_week, 0)::double precision as day_of_week,
        COALESCE(is_weekend, false)::int as is_weekend,
        COALESCE(is_night, false)::int as is_night,
        COALESCE(minutes_since_last_txn, 0)::double precision as minutes_since_last_txn,
        COALESCE(channel_encoded, 0)::double precision as channel_encoded,
        COALESCE(label, false)::int as label
    FROM transaction_features
    ORDER BY transaction_id ASC
    LIMIT %s
    """
    
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()
        conn.close()
        
        if not rows:
            log.warning("No data found in transaction_features")
            return None
        
        df = pd.DataFrame(rows)
        
        # Convert any remaining Decimal to float
        df = convert_decimals(df)
        
        # Ensure numeric types
        df = ensure_numeric(df)
        
        min_samples = config.drift.min_samples
        if len(df) < min_samples:
            log.warning(
                f"Not enough reference data: {len(df)} rows "
                f"(need {min_samples})"
            )
            return None
        
        log.info(f"Loaded {len(df)} reference rows")
        return df
        
    except Exception as e:
        log.exception(f"Failed to load reference data: {e}")
        return None


def load_current_data(limit: Optional[int] = None) -> Optional[pd.DataFrame]:
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
    
    # Query only the features we need for drift detection
    # Use transaction_id for ordering (it's sequential) - DESC for newest
    # Cast NUMERIC to DOUBLE PRECISION to get float instead of Decimal
    query = """
    SELECT 
        transaction_id,
        amount::double precision as amount,
        COALESCE(amount_zscore, 0)::double precision as amount_zscore,
        COALESCE(user_avg_amount_30d, 0)::double precision as user_avg_amount_30d,
        COALESCE(user_txn_count_1h, 0)::double precision as user_txn_count_1h,
        COALESCE(user_txn_count_24h, 0)::double precision as user_txn_count_24h,
        COALESCE(user_txn_count_7d, 0)::double precision as user_txn_count_7d,
        COALESCE(user_amount_sum_1h, 0)::double precision as user_amount_sum_1h,
        COALESCE(user_amount_sum_24h, 0)::double precision as user_amount_sum_24h,
        COALESCE(country_change_flag, false)::int as country_change_flag,
        COALESCE(device_change_flag, false)::int as device_change_flag,
        COALESCE(unique_countries_24h, 1)::double precision as unique_countries_24h,
        COALESCE(unique_merchants_24h, 1)::double precision as unique_merchants_24h,
        COALESCE(user_merchant_first_time, false)::int as user_merchant_first_time,
        COALESCE(hour_of_day, 12)::double precision as hour_of_day,
        COALESCE(day_of_week, 0)::double precision as day_of_week,
        COALESCE(is_weekend, false)::int as is_weekend,
        COALESCE(is_night, false)::int as is_night,
        COALESCE(minutes_since_last_txn, 0)::double precision as minutes_since_last_txn,
        COALESCE(channel_encoded, 0)::double precision as channel_encoded,
        COALESCE(label, false)::int as label
    FROM transaction_features
    ORDER BY transaction_id DESC
    LIMIT %s
    """
    
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()
        conn.close()
        
        if not rows:
            log.warning("No data found in transaction_features")
            return None
        
        df = pd.DataFrame(rows)
        
        # Convert any remaining Decimal to float
        df = convert_decimals(df)
        
        # Ensure numeric types
        df = ensure_numeric(df)
        
        min_samples = config.drift.min_samples
        if len(df) < min_samples:
            log.warning(
                f"Not enough current data: {len(df)} rows "
                f"(need {min_samples})"
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


def get_data_stats() -> Dict[str, Any]:
    """
    Get statistics about available data.
    
    Returns:
        Dictionary with counts and date ranges
    """
    query = """
    SELECT 
        COUNT(*) as total_rows,
        MIN(transaction_id) as oldest_txn,
        MAX(transaction_id) as newest_txn
    FROM transaction_features
    """
    
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()
        conn.close()
        
        if result is None:
            return {"error": "No data found"}
        
        return {
            "total_rows": result["total_rows"],
            "oldest_transaction": result["oldest_txn"],
            "newest_transaction": result["newest_txn"],
            "reference_size": config.drift.reference_window_size,
            "current_size": config.drift.current_window_size,
            "min_samples": config.drift.min_samples,
        }
        
    except Exception as e:
        log.exception(f"Failed to get data stats: {e}")
        return {"error": str(e)}