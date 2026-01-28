"""
MODEL SERVICE CONFIGURATION
================================================================================

Centralized configuration for the scoring API.
All settings can be overridden via environment variables.

================================================================================
"""

import os
from dataclasses import dataclass


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
    """MLflow settings."""
    tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    model_name: str = os.getenv("MODEL_NAME", "fraud-detector-xgboost")
    model_stage: str = os.getenv("MODEL_STAGE", "None")  # "None", "Staging", "Production"
    model_version: str = os.getenv("MODEL_VERSION", "1")  # Specific version to load


@dataclass 
class ScoringConfig:
    """Scoring thresholds and settings."""
    # Decision thresholds
    threshold_review: float = float(os.getenv("THRESHOLD_REVIEW", "0.3"))
    threshold_block: float = float(os.getenv("THRESHOLD_BLOCK", "0.7"))
    
    # Number of top features to return for explanation
    top_k_features: int = int(os.getenv("TOP_K_FEATURES", "5"))
    
    # Whether to create alerts for REVIEW and BLOCK decisions
    create_alerts: bool = os.getenv("CREATE_ALERTS", "true").lower() == "true"


@dataclass
class Config:
    """Master configuration."""
    db: DatabaseConfig = None
    mlflow: MLflowConfig = None
    scoring: ScoringConfig = None
    
    def __post_init__(self):
        self.db = DatabaseConfig()
        self.mlflow = MLflowConfig()
        self.scoring = ScoringConfig()


# Global config instance
config = Config()
