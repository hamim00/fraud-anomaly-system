"""
MODEL SERVICE - FastAPI Scoring API
================================================================================

This is the main API that banks/systems call to score transactions in real-time.

ENDPOINTS:
----------
POST /score          - Score a single transaction
GET  /health         - Health check
GET  /alerts         - Get recent alerts
GET  /alerts/stats   - Get alert statistics

FLOW:
-----
1. Receive transaction data
2. Prepare features (use provided or calculate defaults)
3. Score with XGBoost model
4. Make decision (APPROVE/REVIEW/BLOCK)
5. Generate explanations
6. Create alert if REVIEW or BLOCK
7. Return response

================================================================================
"""

import logging
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional
from contextlib import asynccontextmanager

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

from config import config
from schemas import (
    ScoreRequest, 
    ScoreResponse, 
    HealthResponse, 
    Decision,
    FeatureContribution,
    AlertResponse,
)
from model_loader import model_loader
from explainer import get_feature_contributions, get_risk_factors
from alert_writer import create_alert, check_connection, get_recent_alerts, get_alert_stats

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("model_service")


# ============================================================================
# STARTUP / SHUTDOWN
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model on startup."""
    log.info("=" * 60)
    log.info("STARTING MODEL SERVICE")
    log.info("=" * 60)
    
    # Load model from MLflow
    log.info(f"MLflow URI: {config.mlflow.tracking_uri}")
    log.info(f"Model: {config.mlflow.model_name} v{config.mlflow.model_version}")
    
    success = model_loader.load()
    if not success:
        log.error("Failed to load model! Service will return errors.")
    else:
        log.info("Model loaded successfully!")
    
    # Check database
    if check_connection():
        log.info("Database connection OK")
    else:
        log.warning("Database connection failed! Alerts will not be saved.")
    
    log.info("=" * 60)
    log.info(f"Thresholds: REVIEW >= {config.scoring.threshold_review}, BLOCK >= {config.scoring.threshold_block}")
    log.info("=" * 60)
    
    yield  # Server is running
    
    # Shutdown
    log.info("Shutting down model service...")


# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(
    title="Fraud Detection Scoring API",
    description="Real-time fraud scoring for transactions",
    version="1.0.0",
    lifespan=lifespan,
)

# Allow CORS for testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def prepare_features(request: ScoreRequest) -> pd.DataFrame:
    """
    Prepare features for model scoring.
    
    Uses features from request if provided, otherwise uses defaults.
    In production, these would come from the feature store.
    """
    # Channel encoding
    channel_map = {"POS": 0, "ECOM": 1, "ATM": 2}
    channel_encoded = channel_map.get(request.channel.upper(), 0)
    
    # Current time features
    now = datetime.now(timezone.utc)
    hour_of_day = now.hour
    day_of_week = now.weekday()
    is_weekend = day_of_week >= 5
    is_night = hour_of_day < 6
    
    # Build feature dict
    features = {
        "amount": request.amount,
        "amount_zscore": request.amount_zscore if request.amount_zscore is not None else 0.0,
        "user_avg_amount_30d": request.user_avg_amount_30d if request.user_avg_amount_30d is not None else request.amount,
        "user_txn_count_1h": request.user_txn_count_1h if request.user_txn_count_1h is not None else 0,
        "user_txn_count_24h": request.user_txn_count_24h if request.user_txn_count_24h is not None else 1,
        "user_txn_count_7d": request.user_txn_count_7d if request.user_txn_count_7d is not None else 1,
        "user_amount_sum_1h": request.user_amount_sum_1h if request.user_amount_sum_1h is not None else request.amount,
        "user_amount_sum_24h": request.user_amount_sum_24h if request.user_amount_sum_24h is not None else request.amount,
        "country_change_flag": 1 if request.country_change_flag else 0,
        "device_change_flag": 1 if request.device_change_flag else 0,
        "unique_countries_24h": request.unique_countries_24h if request.unique_countries_24h is not None else 1,
        "unique_merchants_24h": request.unique_merchants_24h if request.unique_merchants_24h is not None else 1,
        "user_merchant_first_time": 1 if request.user_merchant_first_time else 0,
        "hour_of_day": hour_of_day,
        "day_of_week": day_of_week,
        "is_weekend": 1 if is_weekend else 0,
        "is_night": 1 if is_night else 0,
        "minutes_since_last_txn": request.minutes_since_last_txn if request.minutes_since_last_txn is not None else 60,
        "channel_encoded": channel_encoded,
    }
    
    return pd.DataFrame([features])


def make_decision(score: float) -> Decision:
    """
    Make a decision based on score and thresholds.
    
    score < threshold_review      → APPROVE
    threshold_review <= score < threshold_block → REVIEW
    score >= threshold_block      → BLOCK
    """
    if score >= config.scoring.threshold_block:
        return Decision.BLOCK
    elif score >= config.scoring.threshold_review:
        return Decision.REVIEW
    else:
        return Decision.APPROVE


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    
    Returns model status and database connectivity.
    """
    return HealthResponse(
        status="healthy" if model_loader.is_loaded() else "degraded",
        model_loaded=model_loader.is_loaded(),
        model_name=model_loader.model_name,
        model_version=model_loader.model_version,
        database_connected=check_connection(),
    )


@app.post("/score", response_model=ScoreResponse)
async def score_transaction(request: ScoreRequest):
    """
    Score a transaction for fraud probability.
    
    Returns:
    - score: Fraud probability (0-1)
    - decision: APPROVE, REVIEW, or BLOCK
    - top_features: Explanation of key contributing factors
    - risk_factors: Human-readable risk descriptions
    """
    start_time = time.time()
    
    # Check if model is loaded
    if not model_loader.is_loaded():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded. Please try again later."
        )
    
    try:
        # Prepare features
        features_df = prepare_features(request)
        
        # Score
        scores = model_loader.predict_proba(features_df)
        score = float(scores[0])
        
        # Make decision
        decision = make_decision(score)
        
        # Calculate latency
        latency_ms = int((time.time() - start_time) * 1000)
        
        # Get explanations
        feature_importances = model_loader.get_feature_importances()
        features_series = features_df.iloc[0]
        
        top_features = get_feature_contributions(
            features_series,
            feature_importances,
            top_k=config.scoring.top_k_features
        )
        
        risk_factors = get_risk_factors(
            features_series,
            score,
            config.scoring.threshold_review
        )
        
        # Create alert if REVIEW or BLOCK
        alert_id = None
        alert_created = False
        
        if decision in [Decision.REVIEW, Decision.BLOCK] and config.scoring.create_alerts:
            alert_id = create_alert(
                transaction_id=request.transaction_id,
                user_id=request.user_id,
                amount=request.amount,
                channel=request.channel,
                country=request.country,
                score=score,
                threshold=config.scoring.threshold_review if decision == Decision.REVIEW else config.scoring.threshold_block,
                decision=decision.value,
                top_features=top_features,
                model_name=model_loader.model_name,
                model_version=model_loader.model_version,
                latency_ms=latency_ms,
            )
            alert_created = alert_id is not None
        
        # Log
        log.info(
            f"Scored: txn={request.transaction_id[:8]}... "
            f"score={score:.4f} decision={decision.value} "
            f"latency={latency_ms}ms alert_id={alert_id}"
        )
        
        return ScoreResponse(
            transaction_id=request.transaction_id,
            score=round(score, 4),
            decision=decision,
            threshold_review=config.scoring.threshold_review,
            threshold_block=config.scoring.threshold_block,
            top_features=top_features,
            risk_factors=risk_factors,
            model_name=model_loader.model_name,
            model_version=model_loader.model_version,
            scored_at=datetime.now(timezone.utc),
            latency_ms=latency_ms,
            alert_created=alert_created,
            alert_id=alert_id,
        )
        
    except Exception as e:
        log.exception(f"Error scoring transaction: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Scoring failed: {str(e)}"
        )


@app.get("/alerts")
async def get_alerts(limit: int = 20):
    """Get recent alerts."""
    alerts = get_recent_alerts(limit)
    return {"alerts": alerts, "count": len(alerts)}


@app.get("/alerts/stats")
async def get_stats():
    """Get alert statistics."""
    stats = get_alert_stats()
    return stats


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "service": "Fraud Detection Scoring API",
        "version": "1.0.0",
        "model": model_loader.model_name,
        "model_version": model_loader.model_version,
        "endpoints": {
            "POST /score": "Score a transaction",
            "GET /health": "Health check",
            "GET /alerts": "Get recent alerts",
            "GET /alerts/stats": "Get alert statistics",
            "GET /docs": "OpenAPI documentation",
        }
    }


# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
