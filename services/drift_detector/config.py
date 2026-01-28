"""
DRIFT DETECTOR - Configuration
===============================

Configuration for the drift detection service.
All settings can be overridden via environment variables.
"""

import os
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """PostgreSQL connection settings."""
    host: str = os.getenv("PGHOST", "postgres")
    port: int = int(os.getenv("PGPORT", "5432"))
    database: str = os.getenv("PGDATABASE", "fraud_db")
    user: str = os.getenv("PGUSER", "fraud")
    password: str = os.getenv("PGPASSWORD", "fraud")
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class DriftConfig:
    """Drift detection settings."""
    
    # How often to run drift checks (in minutes)
    check_interval_minutes: int = int(os.getenv("DRIFT_CHECK_INTERVAL", "5"))
    
    # Minimum samples needed for drift detection
    min_samples: int = int(os.getenv("DRIFT_MIN_SAMPLES", "100"))
    
    # Reference window: how many rows to use as reference (training data)
    reference_window_size: int = int(os.getenv("DRIFT_REFERENCE_SIZE", "5000"))
    
    # Current window: how many recent rows to compare
    current_window_size: int = int(os.getenv("DRIFT_CURRENT_SIZE", "1000"))
    
    # Drift threshold (p-value below this = drift detected)
    # Lower = more sensitive
    drift_threshold: float = float(os.getenv("DRIFT_THRESHOLD", "0.05"))
    
    # Features to monitor for drift
    monitored_features: list = None
    
    def __post_init__(self):
        features_str = os.getenv("DRIFT_MONITORED_FEATURES", "")
        if features_str:
            self.monitored_features = [f.strip() for f in features_str.split(",")]
        else:
            # Default features to monitor
            self.monitored_features = [
                "amount",
                "amount_zscore",
                "user_txn_count_1h",
                "user_txn_count_24h",
                "user_amount_sum_1h",
                "user_amount_sum_24h",
                "unique_countries_24h",
                "unique_merchants_24h",
                "hour_of_day",
                "channel_encoded",
            ]


@dataclass
class ServerConfig:
    """Server settings."""
    api_port: int = int(os.getenv("API_PORT", "8001"))
    metrics_port: int = int(os.getenv("METRICS_PORT", "9094"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


@dataclass
class Config:
    """Main configuration container."""
    db: DatabaseConfig = None
    drift: DriftConfig = None
    server: ServerConfig = None
    
    def __post_init__(self):
        self.db = DatabaseConfig()
        self.drift = DriftConfig()
        self.server = ServerConfig()


# Global config instance
config = Config()
