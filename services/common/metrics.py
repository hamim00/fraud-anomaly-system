"""
Shared Prometheus Metrics for Fraud Detection System
====================================================

This module provides common metrics definitions used across services.
Each service imports and uses the relevant metrics.

Usage:
    from metrics import SCORING_LATENCY, SCORING_REQUESTS
    
    with SCORING_LATENCY.time():
        result = score_transaction()
    
    SCORING_REQUESTS.inc()
"""

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    Info,
    generate_latest,
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    multiprocess,
    REGISTRY,
)

# =============================================================================
# PRODUCER METRICS
# =============================================================================

TRANSACTIONS_PRODUCED = Counter(
    'fraud_transactions_produced_total',
    'Total number of transactions produced to Kafka',
    ['channel', 'country']
)

PRODUCER_ERRORS = Counter(
    'fraud_producer_errors_total',
    'Total number of producer errors',
    ['error_type']
)

PRODUCER_LATENCY = Histogram(
    'fraud_producer_send_latency_seconds',
    'Time to send message to Kafka',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

# =============================================================================
# CONSUMER METRICS (Stream & Feature)
# =============================================================================

STREAM_CONSUMER_MESSAGES = Counter(
    'fraud_stream_consumer_messages_total',
    'Total messages processed by stream consumer',
    ['status']
)

FEATURE_CONSUMER_MESSAGES = Counter(
    'fraud_feature_consumer_messages_total',
    'Total messages processed by feature consumer',
    ['status']
)

CONSUMER_LAG = Gauge(
    'fraud_consumer_lag',
    'Consumer lag (messages behind)',
    ['consumer_group', 'partition']
)

CONSUMER_PROCESSING_TIME = Histogram(
    'fraud_consumer_processing_seconds',
    'Time to process a single message',
    ['consumer_type'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

DB_WRITE_LATENCY = Histogram(
    'fraud_db_write_latency_seconds',
    'Database write latency',
    ['table'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

# =============================================================================
# MODEL SERVICE METRICS
# =============================================================================

SCORING_REQUESTS = Counter(
    'fraud_scoring_requests_total',
    'Total scoring requests received',
    ['status']
)

SCORING_DECISIONS = Counter(
    'fraud_scoring_decisions_total',
    'Scoring decisions by type',
    ['decision']
)

SCORING_LATENCY = Histogram(
    'fraud_scoring_latency_seconds',
    'End-to-end scoring latency',
    buckets=[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 1.0]
)

ALERTS_CREATED = Counter(
    'fraud_alerts_created_total',
    'Total alerts created',
    ['decision']
)

MODEL_LOADED = Gauge(
    'fraud_model_loaded',
    'Whether the model is loaded (1) or not (0)'
)

MODEL_INFO = Info(
    'fraud_model',
    'Information about the loaded model'
)

# Score distribution tracking
SCORE_MEAN = Gauge(
    'fraud_scoring_score_mean',
    'Rolling mean of fraud scores (last 100)'
)

SCORE_MAX = Gauge(
    'fraud_scoring_score_max',
    'Rolling max of fraud scores (last 100)'
)

# =============================================================================
# HELPER CLASS FOR SCORE TRACKING
# =============================================================================

class RollingScoreTracker:
    """Track rolling statistics of fraud scores."""
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.scores: list = []
    
    def add(self, score: float):
        self.scores.append(score)
        if len(self.scores) > self.window_size:
            self.scores.pop(0)
        
        # Update gauges
        if self.scores:
            SCORE_MEAN.set(sum(self.scores) / len(self.scores))
            SCORE_MAX.set(max(self.scores))

score_tracker = RollingScoreTracker()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_metrics():
    """Generate Prometheus metrics output."""
    return generate_latest(REGISTRY)

def get_content_type():
    """Get the content type for Prometheus metrics."""
    return CONTENT_TYPE_LATEST
