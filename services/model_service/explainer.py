"""
EXPLAINER - Generate explanations for fraud predictions
================================================================================

This module provides explanations for why a transaction was flagged.
Instead of using SHAP (which is slow), we use feature importance
weighted by how unusual each feature value is.

================================================================================
"""

import logging
from typing import List, Dict, Any
import numpy as np
import pandas as pd

from schemas import FeatureContribution

log = logging.getLogger("model_service.explainer")


# Human-readable descriptions for features
FEATURE_DESCRIPTIONS = {
    "amount": "Transaction amount",
    "amount_zscore": "Amount compared to user's average",
    "user_avg_amount_30d": "User's typical spending amount",
    "user_txn_count_1h": "Transactions in last hour",
    "user_txn_count_24h": "Transactions in last 24 hours",
    "user_txn_count_7d": "Transactions in last 7 days",
    "user_amount_sum_1h": "Total spent in last hour",
    "user_amount_sum_24h": "Total spent in last 24 hours",
    "country_change_flag": "Country changed from last transaction",
    "device_change_flag": "Device changed from last transaction",
    "unique_countries_24h": "Different countries in 24 hours",
    "unique_merchants_24h": "Different merchants in 24 hours",
    "user_merchant_first_time": "First time at this merchant",
    "hour_of_day": "Hour of transaction",
    "day_of_week": "Day of week",
    "is_weekend": "Weekend transaction",
    "is_night": "Night-time transaction (midnight-6am)",
    "minutes_since_last_txn": "Minutes since last transaction",
    "channel_encoded": "Transaction channel (POS/ECOM/ATM)",
}


def get_feature_contributions(
    features: pd.Series,
    feature_importances: Dict[str, float],
    top_k: int = 5
) -> List[FeatureContribution]:
    """
    Calculate which features contributed most to the prediction.
    
    We approximate contribution as:
    contribution = feature_importance * normalized_value
    
    This isn't as accurate as SHAP but is much faster for real-time scoring.
    
    Args:
        features: Feature values for this transaction (Series)
        feature_importances: Model's feature importances (dict)
        top_k: Number of top features to return
    
    Returns:
        List of FeatureContribution objects
    """
    contributions = []
    
    for feature_name, value in features.items():
        if feature_name not in feature_importances:
            continue
        
        importance = feature_importances[feature_name]
        
        # Approximate contribution
        # For boolean/binary features, contribution = importance if value is 1
        # For numeric features, we scale by how "extreme" the value is
        if isinstance(value, bool):
            contribution = importance if value else 0
        elif value == 0:
            contribution = 0
        else:
            # Simple heuristic: higher absolute values contribute more
            # In a real system, you'd normalize by training data statistics
            contribution = importance * min(abs(value) / 100, 1.0)
        
        # Generate description
        description = generate_description(feature_name, value)
        
        contributions.append(FeatureContribution(
            feature=feature_name,
            value=float(value) if not isinstance(value, bool) else (1.0 if value else 0.0),
            contribution=round(contribution, 4),
            description=description
        ))
    
    # Sort by contribution (descending) and take top K
    contributions.sort(key=lambda x: x.contribution, reverse=True)
    return contributions[:top_k]


def generate_description(feature_name: str, value: Any) -> str:
    """Generate human-readable description for a feature value."""
    
    base_desc = FEATURE_DESCRIPTIONS.get(feature_name, feature_name)
    
    # Boolean features
    if feature_name == "country_change_flag":
        return "Country changed from previous transaction" if value else "Same country as previous"
    
    if feature_name == "device_change_flag":
        return "Different device from previous transaction" if value else "Same device as previous"
    
    if feature_name == "user_merchant_first_time":
        return "First time shopping at this merchant" if value else "Known merchant"
    
    if feature_name == "is_weekend":
        return "Weekend transaction" if value else "Weekday transaction"
    
    if feature_name == "is_night":
        return "Night-time transaction (midnight-6am)" if value else "Daytime transaction"
    
    # Numeric features
    if feature_name == "amount":
        return f"Transaction amount: ${value:,.2f}"
    
    if feature_name == "amount_zscore":
        if value > 3:
            return f"Amount is {value:.1f} std devs above user average (very unusual)"
        elif value > 2:
            return f"Amount is {value:.1f} std devs above user average (unusual)"
        elif value < -2:
            return f"Amount is {abs(value):.1f} std devs below user average"
        else:
            return f"Amount is typical for this user (z-score: {value:.1f})"
    
    if feature_name == "user_txn_count_1h":
        if value > 5:
            return f"High velocity: {int(value)} transactions in last hour"
        else:
            return f"{int(value)} transaction(s) in last hour"
    
    if feature_name == "user_txn_count_24h":
        return f"{int(value)} transaction(s) in last 24 hours"
    
    if feature_name == "minutes_since_last_txn":
        if value is None or value > 10000:
            return "First transaction in a while"
        elif value < 5:
            return f"Only {int(value)} minutes since last transaction (rapid)"
        else:
            return f"{int(value)} minutes since last transaction"
    
    if feature_name == "unique_countries_24h":
        if value > 2:
            return f"Used in {int(value)} different countries in 24h (suspicious)"
        else:
            return f"Used in {int(value)} country/countries in 24h"
    
    if feature_name == "channel_encoded":
        channels = {0: "POS (in-store)", 1: "ECOM (online)", 2: "ATM"}
        channel_name = channels.get(int(value), f"Channel {int(value)}")
        return f"Transaction channel: {channel_name}"
    
    if feature_name == "hour_of_day":
        return f"Transaction at {int(value):02d}:00"
    
    # Default
    return f"{base_desc}: {value}"


def get_risk_factors(
    features: pd.Series,
    score: float,
    threshold_review: float = 0.3
) -> List[str]:
    """
    Generate human-readable risk factors.
    
    Args:
        features: Feature values
        score: Fraud probability score
        threshold_review: Threshold for concern
    
    Returns:
        List of risk factor strings
    """
    risk_factors = []
    
    # Amount-based risks
    amount = features.get("amount", 0)
    if amount > 2000:
        risk_factors.append(f"High transaction amount (${amount:,.2f})")
    
    zscore = features.get("amount_zscore")
    if zscore and zscore > 3:
        risk_factors.append(f"Amount is unusually high for this user ({zscore:.1f} std devs)")
    
    # Geographic risks
    if features.get("country_change_flag"):
        risk_factors.append("Different country from previous transaction")
    
    countries_24h = features.get("unique_countries_24h", 1)
    if countries_24h and countries_24h > 2:
        risk_factors.append(f"Card used in {int(countries_24h)} countries in 24 hours")
    
    # Velocity risks
    txn_count_1h = features.get("user_txn_count_1h", 0)
    if txn_count_1h and txn_count_1h > 5:
        risk_factors.append(f"High transaction velocity ({int(txn_count_1h)} in last hour)")
    
    mins_since_last = features.get("minutes_since_last_txn")
    if mins_since_last is not None and mins_since_last < 5:
        risk_factors.append(f"Rapid transaction ({int(mins_since_last)} minutes since last)")
    
    # Device/merchant risks
    if features.get("device_change_flag"):
        risk_factors.append("New device detected")
    
    if features.get("user_merchant_first_time"):
        risk_factors.append("First transaction at this merchant")
    
    # Channel risks
    channel = features.get("channel_encoded")
    if channel == 1:  # ECOM
        risk_factors.append("Online transaction (card-not-present)")
    
    # Time-based risks
    if features.get("is_night"):
        risk_factors.append("Transaction during night hours (midnight-6am)")
    
    # If no specific risks but high score
    if not risk_factors and score > threshold_review:
        risk_factors.append("Multiple moderate risk factors combined")
    
    return risk_factors
