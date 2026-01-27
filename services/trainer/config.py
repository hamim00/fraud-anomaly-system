"""
TRAINER CONFIGURATION
================================================================================

Centralized configuration for the training pipeline.
All settings can be overridden via environment variables.

================================================================================
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class DatabaseConfig:
    """PostgreSQL connection settings."""
    host: str = os.getenv("PGHOST", "localhost")
    port: int = int(os.getenv("PGPORT", "5432"))
    database: str = os.getenv("PGDATABASE", "fraud_db")
    user: str = os.getenv("PGUSER", "fraud")
    password: str = os.getenv("PGPASSWORD", "fraud")
    
    @property
    def connection_string(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


@dataclass
class MLflowConfig:
    """MLflow tracking settings."""
    tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    experiment_name: str = os.getenv("MLFLOW_EXPERIMENT", "fraud-detection")


@dataclass
class TrainingConfig:
    """Training parameters."""
    
    # Time-based split ratios
    # Train: 70%, Validation: 15%, Test: 15%
    train_ratio: float = 0.70
    val_ratio: float = 0.15
    test_ratio: float = 0.15
    
    # Features to use (must exist in transaction_features table)
    feature_columns: List[str] = field(default_factory=lambda: [
        # Amount features
        "amount",
        "amount_zscore",
        "user_avg_amount_30d",
        
        # Velocity features
        "user_txn_count_1h",
        "user_txn_count_24h",
        "user_txn_count_7d",
        "user_amount_sum_1h",
        "user_amount_sum_24h",
        
        # Behavioral features
        "country_change_flag",
        "device_change_flag",
        "unique_countries_24h",
        "unique_merchants_24h",
        "user_merchant_first_time",
        
        # Time features
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "is_night",
        "minutes_since_last_txn",
        
        # Channel features
        "channel_encoded",
    ])
    
    # Target column
    target_column: str = "label"
    
    # XGBoost parameters
    xgb_params: dict = field(default_factory=lambda: {
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "min_child_weight": 1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "n_jobs": -1,
    })
    
    # Isolation Forest parameters
    iforest_params: dict = field(default_factory=lambda: {
        "n_estimators": 200,
        "contamination": 0.02,  # Expected fraud rate ~2%
        "random_state": 42,
        "n_jobs": -1,
    })
    
    # Promotion thresholds
    # Model will only be promoted to "Production" if it meets these
    min_pr_auc: float = float(os.getenv("MIN_PR_AUC", "0.20"))  # PR-AUC >= 0.20
    min_recall_at_5pct_fpr: float = float(os.getenv("MIN_RECALL", "0.40"))  # Recall >= 40% at 5% FPR


@dataclass
class Config:
    """Master configuration."""
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    mlflow: MLflowConfig = field(default_factory=MLflowConfig)
    training: TrainingConfig = field(default_factory=TrainingConfig)


# Global config instance
config = Config()
