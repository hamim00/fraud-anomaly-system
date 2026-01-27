"""
DATA LOADER - Load features from PostgreSQL with time-based splits
================================================================================

This module handles:
1. Connecting to PostgreSQL
2. Loading transaction_features data
3. Splitting by TIME (not random!) to prevent data leakage

WHY TIME-BASED SPLIT?
---------------------
In fraud detection (and any time-series problem), we can't use random splits.

Example of DATA LEAKAGE with random split:
- Training data has transactions from Jan 15
- Test data has transactions from Jan 10
- Model "learns" patterns that happened BEFORE the test data
- This gives artificially high metrics that won't work in production

Correct approach: Train on PAST, test on FUTURE
- Training: Jan 1-10
- Validation: Jan 11-13
- Test: Jan 14-15

================================================================================
"""

import logging
import hashlib
from typing import Tuple, Optional
from datetime import datetime

import pandas as pd
import psycopg2

from config import config

log = logging.getLogger("trainer.data_loader")


def get_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(config.db.connection_string)


def load_features(min_rows: int = 1000) -> pd.DataFrame:
    """
    Load all features from transaction_features table.
    
    Args:
        min_rows: Minimum rows required (raises error if less)
    
    Returns:
        DataFrame with all features, sorted by event_time
    """
    log.info("Loading features from PostgreSQL...")
    
    query = """
    SELECT 
        transaction_id,
        user_id,
        event_time,
        
        -- Amount features
        amount,
        amount_zscore,
        user_avg_amount_30d,
        user_std_amount_30d,
        
        -- Velocity features
        user_txn_count_1h,
        user_txn_count_24h,
        user_txn_count_7d,
        user_amount_sum_1h,
        user_amount_sum_24h,
        
        -- Behavioral features
        country_change_flag,
        device_change_flag,
        unique_countries_24h,
        unique_merchants_24h,
        unique_devices_24h,
        user_merchant_first_time,
        
        -- Time features
        hour_of_day,
        day_of_week,
        is_weekend,
        is_night,
        minutes_since_last_txn,
        
        -- Channel features
        channel,
        channel_encoded,
        
        -- Geographic
        country,
        is_foreign_txn,
        
        -- Target
        label
    FROM transaction_features
    WHERE label IS NOT NULL
    ORDER BY event_time ASC
    """
    
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    
    log.info(f"Loaded {len(df):,} rows from transaction_features")
    
    if len(df) < min_rows:
        raise ValueError(
            f"Not enough data for training. Got {len(df)} rows, need at least {min_rows}. "
            f"Let the system run longer to collect more data."
        )
    
    # Log class distribution
    fraud_rate = df["label"].mean() * 100
    log.info(f"Class distribution: {fraud_rate:.2f}% fraud, {100-fraud_rate:.2f}% legitimate")
    
    return df


def compute_data_hash(df: pd.DataFrame) -> str:
    """
    Compute a hash of the training data for reproducibility tracking.
    
    This helps us know if two models were trained on the same data.
    """
    # Hash based on transaction IDs and labels
    data_str = df["transaction_id"].astype(str).sum() + df["label"].astype(str).sum()
    return hashlib.sha256(data_str.encode()).hexdigest()[:16]


def time_based_split(
    df: pd.DataFrame,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split data by time (NOT random!).
    
    This is CRITICAL for fraud detection to prevent data leakage.
    
    Args:
        df: DataFrame sorted by event_time
        train_ratio: Fraction for training (default 70%)
        val_ratio: Fraction for validation (default 15%)
        test_ratio: Fraction for test (default 15%)
    
    Returns:
        Tuple of (train_df, val_df, test_df)
    
    Example:
        If data spans Jan 1 to Jan 20:
        - Train: Jan 1 - Jan 14 (70%)
        - Val: Jan 14 - Jan 17 (15%)
        - Test: Jan 17 - Jan 20 (15%)
    """
    assert abs(train_ratio + val_ratio + test_ratio - 1.0) < 0.01, "Ratios must sum to 1"
    
    # Data should already be sorted, but ensure it
    df = df.sort_values("event_time").reset_index(drop=True)
    
    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))
    
    train_df = df.iloc[:train_end].copy()
    val_df = df.iloc[train_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()
    
    # Log the time ranges
    log.info("=" * 60)
    log.info("TIME-BASED SPLIT:")
    log.info(f"  Train: {train_df['event_time'].min()} to {train_df['event_time'].max()}")
    log.info(f"         {len(train_df):,} rows ({len(train_df)/n*100:.1f}%)")
    log.info(f"         Fraud rate: {train_df['label'].mean()*100:.2f}%")
    log.info(f"  Val:   {val_df['event_time'].min()} to {val_df['event_time'].max()}")
    log.info(f"         {len(val_df):,} rows ({len(val_df)/n*100:.1f}%)")
    log.info(f"         Fraud rate: {val_df['label'].mean()*100:.2f}%")
    log.info(f"  Test:  {test_df['event_time'].min()} to {test_df['event_time'].max()}")
    log.info(f"         {len(test_df):,} rows ({len(test_df)/n*100:.1f}%)")
    log.info(f"         Fraud rate: {test_df['label'].mean()*100:.2f}%")
    log.info("=" * 60)
    
    return train_df, val_df, test_df


def prepare_features(
    df: pd.DataFrame, 
    feature_columns: list,
    target_column: str = "label"
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare features for training.
    
    Handles:
    - Selecting only the feature columns
    - Converting booleans to integers
    - Filling missing values
    
    Args:
        df: Input DataFrame
        feature_columns: List of column names to use as features
        target_column: Name of target column
    
    Returns:
        Tuple of (X, y) where X is features DataFrame and y is target Series
    """
    # Select feature columns that exist
    available_features = [col for col in feature_columns if col in df.columns]
    missing_features = [col for col in feature_columns if col not in df.columns]
    
    if missing_features:
        log.warning(f"Missing features (will be skipped): {missing_features}")
    
    X = df[available_features].copy()
    y = df[target_column].copy()
    
    # Convert booleans to integers
    bool_columns = X.select_dtypes(include=['bool']).columns
    for col in bool_columns:
        X[col] = X[col].astype(int)
    
    # Fill missing values with median (for numeric columns)
    for col in X.columns:
        if X[col].isna().any():
            if X[col].dtype in ['int64', 'float64']:
                median_val = X[col].median()
                X[col] = X[col].fillna(median_val)
                log.debug(f"Filled {col} NaN with median: {median_val}")
            else:
                X[col] = X[col].fillna(0)
    
    log.info(f"Prepared {len(available_features)} features: {available_features}")
    
    return X, y


def get_feature_stats(df: pd.DataFrame) -> dict:
    """Get statistics about the features for logging."""
    stats = {
        "total_rows": len(df),
        "fraud_count": int(df["label"].sum()),
        "legitimate_count": int((~df["label"]).sum()),
        "fraud_rate": float(df["label"].mean()),
        "date_range_start": str(df["event_time"].min()),
        "date_range_end": str(df["event_time"].max()),
        "unique_users": int(df["user_id"].nunique()),
    }
    return stats
