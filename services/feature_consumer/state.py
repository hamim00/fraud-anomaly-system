"""
STATE STORE - Keeps track of user transaction history in memory
================================================================================

WHY DO WE NEED THIS?
--------------------
To calculate features like "transactions in last 1 hour", we need to remember
what each user did recently. This module stores that history in memory.

HOW IT WORKS:
-------------
1. For each user, we keep a list of their recent transactions
2. When a new transaction arrives, we add it to the user's history
3. We periodically clean up old transactions to save memory

EXAMPLE:
--------
    state = StateStore()
    
    # User makes a transaction
    state.add_transaction("U000001", {
        "transaction_id": "abc-123",
        "amount": 100.00,
        "event_time": datetime.now(),
        "country": "US",
        ...
    })
    
    # Later, get their history
    history = state.get_user_history("U000001")
    # Returns list of recent transactions for this user

LIMITATIONS:
------------
- Data is lost when service restarts (we'll fix this later with Redis)
- Memory limited (we cap at 100,000 users)
- Only keeps last 7 days of data per user

================================================================================
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Deque
from collections import deque
import threading

log = logging.getLogger("feature_consumer.state")


@dataclass
class Transaction:
    """
    A simple container for transaction data.
    We only store what we need for feature calculation.
    """
    transaction_id: str
    amount: float
    event_time: datetime
    country: str
    merchant_id: str
    channel: str
    device_id: Optional[str]
    
    def to_dict(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "amount": self.amount,
            "event_time": self.event_time,
            "country": self.country,
            "merchant_id": self.merchant_id,
            "channel": self.channel,
            "device_id": self.device_id,
        }


@dataclass
class UserState:
    """
    Stores the transaction history for ONE user.
    
    Uses a deque (double-ended queue) which automatically removes
    old items when it gets too full (maxlen=500).
    """
    # Recent transactions (newest last)
    # maxlen=500 means we keep at most 500 transactions per user
    transactions: Deque[Transaction] = field(
        default_factory=lambda: deque(maxlen=500)
    )
    
    # User's "home" country (most common country in their history)
    home_country: Optional[str] = None
    
    # Set of merchants this user has visited
    known_merchants: set = field(default_factory=set)
    
    # Last transaction time (for calculating time since last)
    last_txn_time: Optional[datetime] = None
    last_country: Optional[str] = None
    last_device: Optional[str] = None
    
    def add_transaction(self, txn: Transaction) -> None:
        """Add a new transaction to this user's history."""
        self.transactions.append(txn)
        self.known_merchants.add(txn.merchant_id)
        
        # Update "last" trackers
        self.last_txn_time = txn.event_time
        self.last_country = txn.country
        self.last_device = txn.device_id
    
    def get_transactions_since(self, since: datetime) -> List[Transaction]:
        """Get all transactions after a certain time."""
        return [t for t in self.transactions if t.event_time >= since]
    
    def get_transaction_count(self, hours: int, current_time: datetime) -> int:
        """Count transactions in the last N hours."""
        cutoff = current_time - timedelta(hours=hours)
        return len([t for t in self.transactions if t.event_time >= cutoff])
    
    def get_amount_sum(self, hours: int, current_time: datetime) -> float:
        """Sum of amounts in the last N hours."""
        cutoff = current_time - timedelta(hours=hours)
        return sum(t.amount for t in self.transactions if t.event_time >= cutoff)
    
    def get_unique_countries(self, hours: int, current_time: datetime) -> int:
        """Count unique countries in the last N hours."""
        cutoff = current_time - timedelta(hours=hours)
        countries = {t.country for t in self.transactions if t.event_time >= cutoff}
        return len(countries)
    
    def get_unique_merchants(self, hours: int, current_time: datetime) -> int:
        """Count unique merchants in the last N hours."""
        cutoff = current_time - timedelta(hours=hours)
        merchants = {t.merchant_id for t in self.transactions if t.event_time >= cutoff}
        return len(merchants)
    
    def get_unique_devices(self, hours: int, current_time: datetime) -> int:
        """Count unique devices in the last N hours."""
        cutoff = current_time - timedelta(hours=hours)
        devices = {t.device_id for t in self.transactions 
                   if t.event_time >= cutoff and t.device_id is not None}
        return len(devices)
    
    def get_amount_stats(self, days: int, current_time: datetime) -> tuple:
        """
        Calculate mean and standard deviation of amounts over N days.
        Returns (mean, std_dev) or (None, None) if not enough data.
        """
        cutoff = current_time - timedelta(days=days)
        amounts = [t.amount for t in self.transactions if t.event_time >= cutoff]
        
        if len(amounts) < 2:
            return None, None
        
        mean = sum(amounts) / len(amounts)
        variance = sum((x - mean) ** 2 for x in amounts) / len(amounts)
        std_dev = variance ** 0.5
        
        return mean, std_dev
    
    def is_merchant_first_time(self, merchant_id: str) -> bool:
        """Check if this is the first time user visits this merchant."""
        return merchant_id not in self.known_merchants


class StateStore:
    """
    The main state store that holds history for ALL users.
    
    Thread-safe: Uses a lock for concurrent access.
    Memory-bounded: Limits total users to prevent memory exhaustion.
    
    USAGE:
    ------
        store = StateStore(max_users=100_000)
        
        # Add a transaction
        user_state = store.get_or_create_user("U000001")
        user_state.add_transaction(txn)
        
        # Get features
        count_1h = user_state.get_transaction_count(hours=1, current_time=now)
    """
    
    def __init__(self, max_users: int = 100_000, cleanup_interval_minutes: int = 10):
        """
        Initialize the state store.
        
        Args:
            max_users: Maximum number of users to track (prevents memory issues)
            cleanup_interval_minutes: How often to clean up old data
        """
        self.max_users = max_users
        self.cleanup_interval = timedelta(minutes=cleanup_interval_minutes)
        
        # Dictionary mapping user_id -> UserState
        self._users: Dict[str, UserState] = {}
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Track when we last cleaned up
        self._last_cleanup = datetime.now(timezone.utc)
        
        log.info("StateStore initialized: max_users=%d", max_users)
    
    def get_or_create_user(self, user_id: str) -> UserState:
        """
        Get a user's state, creating it if it doesn't exist.
        
        Args:
            user_id: The user identifier (e.g., "U000001")
            
        Returns:
            UserState object for this user
        """
        with self._lock:
            if user_id not in self._users:
                # Check if we need to evict old users
                if len(self._users) >= self.max_users:
                    self._evict_oldest_users(count=1000)
                
                self._users[user_id] = UserState()
            
            return self._users[user_id]
    
    def add_transaction(self, user_id: str, txn_data: dict) -> UserState:
        """
        Add a transaction and return the user's state.
        
        This is the main method you'll call from the consumer.
        
        Args:
            user_id: User identifier
            txn_data: Transaction data dict with keys:
                      transaction_id, amount, event_time, country,
                      merchant_id, channel, device_id
        
        Returns:
            UserState with the transaction added
        """
        txn = Transaction(
            transaction_id=txn_data["transaction_id"],
            amount=float(txn_data["amount"]),
            event_time=txn_data["event_time"],
            country=txn_data["country"],
            merchant_id=txn_data["merchant_id"],
            channel=txn_data["channel"],
            device_id=txn_data.get("device_id"),
        )
        
        user_state = self.get_or_create_user(user_id)
        user_state.add_transaction(txn)
        
        # Periodic cleanup
        self._maybe_cleanup()
        
        return user_state
    
    def get_user_count(self) -> int:
        """Get the number of users currently being tracked."""
        with self._lock:
            return len(self._users)
    
    def _evict_oldest_users(self, count: int = 1000) -> None:
        """
        Remove users with oldest last transaction time.
        Called when we hit max_users limit.
        """
        with self._lock:
            # Find users with oldest last_txn_time
            users_by_time = []
            for user_id, state in self._users.items():
                if state.last_txn_time:
                    users_by_time.append((state.last_txn_time, user_id))
            
            # Sort by time (oldest first)
            users_by_time.sort()
            
            # Remove oldest N users
            for _, user_id in users_by_time[:count]:
                del self._users[user_id]
            
            log.info("Evicted %d oldest users. Remaining: %d", 
                     min(count, len(users_by_time)), len(self._users))
    
    def _maybe_cleanup(self) -> None:
        """Periodically clean up old transactions from user histories."""
        now = datetime.now(timezone.utc)
        
        if now - self._last_cleanup < self.cleanup_interval:
            return
        
        with self._lock:
            self._last_cleanup = now
            cutoff = now - timedelta(days=7)
            
            # For each user, remove transactions older than 7 days
            users_to_remove = []
            for user_id, state in self._users.items():
                # Filter out old transactions
                old_count = len(state.transactions)
                state.transactions = deque(
                    (t for t in state.transactions if t.event_time >= cutoff),
                    maxlen=500
                )
                
                # If user has no recent activity, mark for removal
                if len(state.transactions) == 0:
                    users_to_remove.append(user_id)
            
            # Remove inactive users
            for user_id in users_to_remove:
                del self._users[user_id]
            
            if users_to_remove:
                log.info("Cleanup: removed %d inactive users", len(users_to_remove))
