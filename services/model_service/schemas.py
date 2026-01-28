"""
API SCHEMAS - Pydantic models for request/response validation
================================================================================

These models define the exact structure of:
1. What the API expects as input (ScoreRequest)
2. What the API returns as output (ScoreResponse)

Pydantic automatically:
- Validates data types
- Returns clear error messages
- Generates OpenAPI documentation

================================================================================
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


class Decision(str, Enum):
    """Possible decisions for a transaction."""
    APPROVE = "APPROVE"   # Low risk - approve the transaction
    REVIEW = "REVIEW"     # Medium risk - send to manual review
    BLOCK = "BLOCK"       # High risk - block the transaction


class FeatureContribution(BaseModel):
    """
    Explains why a feature contributed to the score.
    
    Example:
        {
            "feature": "amount",
            "value": 5000.00,
            "contribution": 0.35,
            "description": "Amount is 8.5 std devs above user average"
        }
    """
    feature: str = Field(..., description="Feature name")
    value: float = Field(..., description="Feature value for this transaction")
    contribution: float = Field(..., description="How much this feature contributed to score (0-1)")
    description: Optional[str] = Field(None, description="Human-readable explanation")


class ScoreRequest(BaseModel):
    """
    Request body for scoring a transaction.
    
    This should contain all the features needed by the model.
    The API will calculate derived features if needed.
    """
    # Required identifiers
    transaction_id: str = Field(..., description="Unique transaction ID")
    user_id: str = Field(..., description="User identifier")
    
    # Transaction details
    amount: float = Field(..., gt=0, description="Transaction amount")
    currency: str = Field(default="USD", description="Currency code")
    channel: str = Field(..., description="Channel: POS, ECOM, or ATM")
    country: str = Field(..., description="Country code (e.g., US, GB, BD)")
    merchant_id: str = Field(..., description="Merchant identifier")
    
    # Optional fields
    device_id: Optional[str] = Field(None, description="Device identifier")
    ip_hash: Optional[str] = Field(None, description="Hashed IP address")
    city: Optional[str] = Field(None, description="City name")
    
    # Pre-calculated features (optional - will be calculated if not provided)
    # These would come from the feature_consumer in a full pipeline
    amount_zscore: Optional[float] = Field(None, description="Z-score of amount vs user average")
    user_txn_count_1h: Optional[int] = Field(None, description="User transactions in last 1 hour")
    user_txn_count_24h: Optional[int] = Field(None, description="User transactions in last 24 hours")
    user_txn_count_7d: Optional[int] = Field(None, description="User transactions in last 7 days")
    user_amount_sum_1h: Optional[float] = Field(None, description="User amount sum in last 1 hour")
    user_amount_sum_24h: Optional[float] = Field(None, description="User amount sum in last 24 hours")
    user_avg_amount_30d: Optional[float] = Field(None, description="User average amount over 30 days")
    country_change_flag: Optional[bool] = Field(None, description="Country changed from last transaction")
    device_change_flag: Optional[bool] = Field(None, description="Device changed from last transaction")
    unique_countries_24h: Optional[int] = Field(None, description="Unique countries in last 24 hours")
    unique_merchants_24h: Optional[int] = Field(None, description="Unique merchants in last 24 hours")
    user_merchant_first_time: Optional[bool] = Field(None, description="First time at this merchant")
    minutes_since_last_txn: Optional[int] = Field(None, description="Minutes since last transaction")
    
    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "txn-12345-abcde",
                "user_id": "U000042",
                "amount": 2500.00,
                "currency": "USD",
                "channel": "ECOM",
                "country": "US",
                "merchant_id": "M000123",
                "device_id": "D000999",
                "amount_zscore": 3.5,
                "user_txn_count_1h": 5,
                "country_change_flag": True
            }
        }


class ScoreResponse(BaseModel):
    """
    Response from the scoring API.
    
    Contains:
    - The fraud probability score
    - The decision (APPROVE/REVIEW/BLOCK)
    - Explanations for the decision
    - Metadata about the scoring
    """
    # Transaction reference
    transaction_id: str = Field(..., description="Transaction ID from request")
    
    # Scoring results
    score: float = Field(..., ge=0, le=1, description="Fraud probability (0-1)")
    decision: Decision = Field(..., description="Decision: APPROVE, REVIEW, or BLOCK")
    threshold_review: float = Field(..., description="Threshold for REVIEW decision")
    threshold_block: float = Field(..., description="Threshold for BLOCK decision")
    
    # Explainability
    top_features: List[FeatureContribution] = Field(
        ..., 
        description="Top features contributing to the score"
    )
    risk_factors: List[str] = Field(
        ...,
        description="Human-readable risk factors"
    )
    
    # Metadata
    model_name: str = Field(..., description="Model used for scoring")
    model_version: str = Field(..., description="Model version")
    scored_at: datetime = Field(..., description="When scoring occurred")
    latency_ms: int = Field(..., description="Scoring latency in milliseconds")
    
    # Alert info (if created)
    alert_created: bool = Field(..., description="Whether an alert was created")
    alert_id: Optional[int] = Field(None, description="Alert ID if created")
    
    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "txn-12345-abcde",
                "score": 0.847,
                "decision": "BLOCK",
                "threshold_review": 0.3,
                "threshold_block": 0.7,
                "top_features": [
                    {"feature": "amount", "value": 5000.0, "contribution": 0.35, "description": "High amount"},
                    {"feature": "country_change_flag", "value": 1.0, "contribution": 0.28, "description": "Country changed"},
                    {"feature": "channel_encoded", "value": 1.0, "contribution": 0.15, "description": "Online purchase"}
                ],
                "risk_factors": [
                    "High transaction amount ($5,000)",
                    "Different country from last transaction",
                    "Online purchase (higher risk channel)"
                ],
                "model_name": "fraud-detector-xgboost",
                "model_version": "1",
                "scored_at": "2026-01-27T21:30:00Z",
                "latency_ms": 23,
                "alert_created": True,
                "alert_id": 12345
            }
        }


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    model_loaded: bool
    model_name: Optional[str]
    model_version: Optional[str]
    database_connected: bool


class AlertResponse(BaseModel):
    """Response when querying alerts."""
    id: int
    transaction_id: str
    score: float
    decision: str
    created_at: datetime
    user_id: str
    amount: float
    resolution: Optional[str]
