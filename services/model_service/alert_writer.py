"""
ALERT WRITER - Save fraud alerts to PostgreSQL
================================================================================

When a transaction is flagged for REVIEW or BLOCK, we create an alert
in the database. This allows the fraud investigation team to:
1. Review suspicious transactions
2. Mark them as CONFIRMED_FRAUD or FALSE_POSITIVE
3. Track model performance over time

================================================================================
"""

import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

import psycopg2
import psycopg2.extras

from config import config
from schemas import ScoreResponse, FeatureContribution

log = logging.getLogger("model_service.alert_writer")


# Global connection (reused across requests)
_connection = None


def get_connection():
    """Get or create database connection."""
    global _connection
    
    if _connection is None or _connection.closed:
        _connection = psycopg2.connect(config.db.connection_string)
        _connection.autocommit = False
        log.info("Database connection established")
    
    return _connection


def check_connection() -> bool:
    """Check if database is reachable."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception as e:
        log.error(f"Database connection check failed: {e}")
        return False


INSERT_ALERT_SQL = """
INSERT INTO alerts (
    transaction_id,
    user_id,
    amount,
    channel,
    country,
    score,
    threshold,
    decision,
    top_features,
    model_name,
    model_version,
    inference_latency_ms
)
VALUES (
    %(transaction_id)s,
    %(user_id)s,
    %(amount)s,
    %(channel)s,
    %(country)s,
    %(score)s,
    %(threshold)s,
    %(decision)s,
    %(top_features)s,
    %(model_name)s,
    %(model_version)s,
    %(latency_ms)s
)
RETURNING id;
"""


def create_alert(
    transaction_id: str,
    user_id: str,
    amount: float,
    channel: str,
    country: str,
    score: float,
    threshold: float,
    decision: str,
    top_features: List[FeatureContribution],
    model_name: str,
    model_version: str,
    latency_ms: int
) -> Optional[int]:
    """
    Create an alert in the database.
    
    Args:
        transaction_id: Transaction identifier
        user_id: User identifier
        amount: Transaction amount
        channel: Transaction channel (POS/ECOM/ATM)
        country: Country code
        score: Fraud probability score
        threshold: Threshold used for decision
        decision: APPROVE/REVIEW/BLOCK
        top_features: List of feature contributions
        model_name: Model name
        model_version: Model version
        latency_ms: Scoring latency
    
    Returns:
        Alert ID if created, None if failed
    """
    try:
        conn = get_connection()
        
        # Convert feature contributions to JSON
        features_json = [
            {
                "feature": f.feature,
                "value": f.value,
                "contribution": f.contribution,
                "description": f.description
            }
            for f in top_features
        ]
        
        with conn.cursor() as cur:
            cur.execute(INSERT_ALERT_SQL, {
                "transaction_id": transaction_id,
                "user_id": user_id,
                "amount": amount,
                "channel": channel,
                "country": country,
                "score": score,
                "threshold": threshold,
                "decision": decision,
                "top_features": psycopg2.extras.Json(features_json),
                "model_name": model_name,
                "model_version": model_version,
                "latency_ms": latency_ms,
            })
            
            alert_id = cur.fetchone()[0]
        
        conn.commit()
        log.info(f"Alert created: id={alert_id}, txn={transaction_id}, decision={decision}, score={score:.4f}")
        
        return alert_id
        
    except Exception as e:
        log.exception(f"Failed to create alert: {e}")
        try:
            conn.rollback()
        except:
            pass
        return None


def get_recent_alerts(limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent alerts for monitoring."""
    try:
        conn = get_connection()
        
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    id, transaction_id, user_id, amount, channel, country,
                    score, decision, created_at, resolution
                FROM alerts
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            
            return [dict(row) for row in cur.fetchall()]
            
    except Exception as e:
        log.exception(f"Failed to get recent alerts: {e}")
        return []


def get_alert_stats() -> Dict[str, Any]:
    """Get alert statistics for monitoring."""
    try:
        conn = get_connection()
        
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Overall counts
            cur.execute("""
                SELECT 
                    COUNT(*) as total_alerts,
                    COUNT(*) FILTER (WHERE decision = 'BLOCK') as blocked,
                    COUNT(*) FILTER (WHERE decision = 'REVIEW') as review,
                    COUNT(*) FILTER (WHERE decision = 'APPROVE') as approved,
                    COUNT(*) FILTER (WHERE resolution IS NULL) as unresolved,
                    AVG(score) as avg_score,
                    AVG(inference_latency_ms) as avg_latency_ms
                FROM alerts
            """)
            
            stats = dict(cur.fetchone())
            
            # Last hour stats
            cur.execute("""
                SELECT COUNT(*) as alerts_last_hour
                FROM alerts
                WHERE created_at > now() - interval '1 hour'
            """)
            
            stats.update(dict(cur.fetchone()))
            
            return stats
            
    except Exception as e:
        log.exception(f"Failed to get alert stats: {e}")
        return {}
