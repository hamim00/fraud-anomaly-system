"""
FEATURE CALCULATOR - Computes ML features for each transaction
================================================================================

This module contains the logic to calculate features that help detect fraud.
Each feature is a "clue" that the ML model will use.

FEATURE CATEGORIES:
-------------------
1. VELOCITY FEATURES: How fast is the user transacting?
   - High velocity = possible stolen card being used quickly

2. AMOUNT FEATURES: Is this amount unusual for this user?
   - Sudden large purchases = suspicious

3. BEHAVIORAL FEATURES: Is user acting differently?
   - Different country, device, merchant patterns = suspicious

4. TIME FEATURES: When is the transaction happening?
   - 3 AM transactions might be more suspicious

USAGE:
------
    from features import FeatureCalculator
    from state import StateStore
    
    store = StateStore()
    calculator = FeatureCalculator()
    
    # For each transaction:
    user_state = store.add_transaction(user_id, txn_data)
    features = calculator.calculate(txn_data, user_state)
    # features is a dict ready to insert into database

================================================================================
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from dataclasses import dataclass

from state import UserState

log = logging.getLogger("feature_consumer.features")


@dataclass
class FeatureConfig:
    """Configuration for feature calculation."""
    
    # Time windows for velocity features
    velocity_windows_hours: tuple = (1, 24, 168)  # 1h, 24h, 7d
    
    # For amount statistics
    amount_history_days: int = 30
    
    # Minimum transactions needed for statistics
    min_transactions_for_stats: int = 3
    
    # Channel encoding
    channel_encoding: dict = None
    
    def __post_init__(self):
        if self.channel_encoding is None:
            self.channel_encoding = {
                "POS": 0,
                "ECOM": 1,
                "ATM": 2,
            }


class FeatureCalculator:
    """
    Calculates features for a transaction based on user history.
    
    EXAMPLE:
    --------
        calculator = FeatureCalculator()
        
        txn = {
            "transaction_id": "abc-123",
            "user_id": "U000001",
            "amount": 500.00,
            "event_time": datetime.now(),
            "country": "US",
            "merchant_id": "M000001",
            "channel": "ECOM",
            "device_id": "D123",
            "label": False
        }
        
        user_state = store.get_or_create_user("U000001")
        features = calculator.calculate(txn, user_state)
        
        # features dict contains all computed features
    """
    
    def __init__(self, config: Optional[FeatureConfig] = None):
        self.config = config or FeatureConfig()
        log.info("FeatureCalculator initialized")
    
    def calculate(self, txn: Dict[str, Any], user_state: UserState) -> Dict[str, Any]:
        """
        Calculate all features for a transaction.
        
        Args:
            txn: Transaction data dictionary
            user_state: User's historical state (BEFORE this transaction is added)
        
        Returns:
            Dictionary of features ready for database insertion
        """
        event_time = txn["event_time"]
        if isinstance(event_time, str):
            event_time = datetime.fromisoformat(event_time)
        
        # Ensure timezone aware
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        
        amount = float(txn["amount"])
        
        # ====================================================================
        # Build features dictionary
        # ====================================================================
        features = {
            # Identifiers
            "transaction_id": txn["transaction_id"],
            "user_id": txn["user_id"],
            "event_time": event_time,
            
            # Original data
            "amount": amount,
            "channel": txn["channel"],
            "country": txn["country"],
            "label": txn.get("label"),
        }
        
        # ====================================================================
        # VELOCITY FEATURES
        # ====================================================================
        features.update(self._calculate_velocity_features(user_state, event_time))
        
        # ====================================================================
        # AMOUNT FEATURES  
        # ====================================================================
        features.update(self._calculate_amount_features(amount, user_state, event_time))
        
        # ====================================================================
        # BEHAVIORAL FEATURES
        # ====================================================================
        features.update(self._calculate_behavioral_features(txn, user_state, event_time))
        
        # ====================================================================
        # TIME FEATURES
        # ====================================================================
        features.update(self._calculate_time_features(event_time, user_state))
        
        # ====================================================================
        # CHANNEL FEATURES
        # ====================================================================
        features.update(self._calculate_channel_features(txn))
        
        return features
    
    def _calculate_velocity_features(
        self, 
        user_state: UserState, 
        event_time: datetime
    ) -> Dict[str, Any]:
        """
        Calculate velocity features (how fast user is transacting).
        
        High velocity might indicate:
        - Stolen card being used rapidly
        - Bot/automated fraud
        """
        features = {}
        
        # Transaction counts
        features["user_txn_count_1h"] = user_state.get_transaction_count(
            hours=1, current_time=event_time
        )
        features["user_txn_count_24h"] = user_state.get_transaction_count(
            hours=24, current_time=event_time
        )
        features["user_txn_count_7d"] = user_state.get_transaction_count(
            hours=168, current_time=event_time  # 7 * 24 = 168
        )
        
        # Amount sums
        features["user_amount_sum_1h"] = round(
            user_state.get_amount_sum(hours=1, current_time=event_time), 2
        )
        features["user_amount_sum_24h"] = round(
            user_state.get_amount_sum(hours=24, current_time=event_time), 2
        )
        
        return features
    
    def _calculate_amount_features(
        self,
        amount: float,
        user_state: UserState,
        event_time: datetime
    ) -> Dict[str, Any]:
        """
        Calculate amount-related features.
        
        Unusual amounts might indicate:
        - Stolen card used for large purchase
        - Testing card with small amounts
        """
        features = {}
        
        # Get historical statistics
        mean, std = user_state.get_amount_stats(
            days=self.config.amount_history_days,
            current_time=event_time
        )
        
        features["user_avg_amount_30d"] = round(mean, 2) if mean else None
        features["user_std_amount_30d"] = round(std, 2) if std else None
        
        # Calculate Z-score
        # Z-score = (value - mean) / std_dev
        # High z-score means this amount is unusual for this user
        if mean is not None and std is not None and std > 0:
            zscore = (amount - mean) / std
            features["amount_zscore"] = round(zscore, 4)
        else:
            features["amount_zscore"] = None
        
        return features
    
    def _calculate_behavioral_features(
        self,
        txn: Dict[str, Any],
        user_state: UserState,
        event_time: datetime
    ) -> Dict[str, Any]:
        """
        Calculate behavioral features (is user acting differently?).
        
        Behavioral changes might indicate:
        - Account takeover
        - Card stolen and used in different location
        """
        features = {}
        
        # Country change detection
        # If user's last transaction was in Bangladesh, and now it's US, suspicious!
        features["country_change_flag"] = (
            user_state.last_country is not None 
            and user_state.last_country != txn["country"]
        )
        
        # Device change detection
        current_device = txn.get("device_id")
        features["device_change_flag"] = (
            user_state.last_device is not None
            and current_device is not None
            and user_state.last_device != current_device
        )
        
        # Unique counts (diversity features)
        features["unique_countries_24h"] = user_state.get_unique_countries(
            hours=24, current_time=event_time
        )
        features["unique_merchants_24h"] = user_state.get_unique_merchants(
            hours=24, current_time=event_time
        )
        features["unique_devices_24h"] = user_state.get_unique_devices(
            hours=24, current_time=event_time
        )
        
        # First time at merchant?
        features["user_merchant_first_time"] = user_state.is_merchant_first_time(
            txn["merchant_id"]
        )
        
        # Is this a foreign transaction?
        # (different from user's most common country)
        if user_state.home_country:
            features["is_foreign_txn"] = txn["country"] != user_state.home_country
        else:
            features["is_foreign_txn"] = None
        
        return features
    
    def _calculate_time_features(
        self,
        event_time: datetime,
        user_state: UserState
    ) -> Dict[str, Any]:
        """
        Calculate time-based features.
        
        Time patterns might reveal:
        - Unusual hours (3 AM transactions)
        - Weekend patterns
        - Rapid transactions (time since last)
        """
        features = {}
        
        # Hour of day (0-23)
        features["hour_of_day"] = event_time.hour
        
        # Day of week (0=Monday, 6=Sunday)
        features["day_of_week"] = event_time.weekday()
        
        # Is weekend?
        features["is_weekend"] = event_time.weekday() >= 5
        
        # Is night? (00:00 to 06:00)
        features["is_night"] = event_time.hour < 6
        
        # Minutes since last transaction
        if user_state.last_txn_time:
            delta = event_time - user_state.last_txn_time
            minutes = int(delta.total_seconds() / 60)
            # Cap at reasonable max (1 week in minutes)
            features["minutes_since_last_txn"] = min(minutes, 10080)
        else:
            # First transaction for this user
            features["minutes_since_last_txn"] = None
        
        return features
    
    def _calculate_channel_features(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate channel-related features.
        
        Different channels have different risk profiles:
        - POS: Physical card present, lower risk
        - ECOM: Online, higher risk (card-not-present)
        - ATM: Cash withdrawal, medium risk
        """
        features = {}
        
        channel = txn["channel"]
        features["channel_encoded"] = self.config.channel_encoding.get(channel, -1)
        
        return features


# =============================================================================
# FEATURE DOCUMENTATION
# =============================================================================
# 
# This section documents what each feature means and why it's useful.
# 
# VELOCITY FEATURES:
# -----------------
# user_txn_count_1h: Transactions in last hour
#   - Normal: 0-2
#   - Suspicious: 5+ (rapid-fire fraud pattern)
# 
# user_txn_count_24h: Transactions in last 24 hours
#   - Normal: 0-10
#   - Suspicious: 20+ (heavy usage, possible fraud spree)
# 
# user_amount_sum_1h: Total spent in last hour
#   - High values relative to user's normal = suspicious
# 
# AMOUNT FEATURES:
# ---------------
# amount_zscore: How many standard deviations from user's average
#   - Normal: -2 to +2
#   - Suspicious: > 3 (very unusual amount)
#   - Example: User normally spends $50, suddenly spends $5000
#             zscore = (5000 - 50) / 30 = 165 (extremely suspicious!)
# 
# BEHAVIORAL FEATURES:
# -------------------
# country_change_flag: Did country change from last transaction?
#   - True + short time gap = very suspicious
#   - Example: Bangladesh at 10:00 AM, US at 10:05 AM (impossible travel)
# 
# unique_countries_24h: Number of different countries in 24h
#   - Normal: 1-2
#   - Suspicious: 3+ (impossible to physically travel that fast)
# 
# user_merchant_first_time: First time at this merchant?
#   - True + high amount = moderate risk
# 
# TIME FEATURES:
# -------------
# minutes_since_last_txn: Time gap from previous transaction
#   - Very small (< 5 min) + different location = suspicious
#   - Very large (> 10080 = 1 week) = first txn in a while
# 
# is_night: Transaction between midnight and 6 AM
#   - Slightly higher risk for certain fraud types
# 
# =============================================================================
