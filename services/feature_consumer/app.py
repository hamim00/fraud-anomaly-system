"""
FEATURE CONSUMER - Main Application
================================================================================

This service does the following:
1. Reads transactions from Kafka (same topic as raw events)
2. For each transaction, calculates features based on user history
3. Saves features to the transaction_features table

ARCHITECTURE:
-------------
                                    ┌─────────────────┐
                                    │  StateStore     │
                                    │  (user history) │
                                    └────────┬────────┘
                                             │
┌──────────┐    ┌──────────────────┐    ┌────▼────────────┐    ┌───────────┐
│  Kafka   │───▶│ Feature Consumer │───▶│FeatureCalculator│───▶│ PostgreSQL│
│  topic   │    │   (this file)    │    │                 │    │  features │
└──────────┘    └──────────────────┘    └─────────────────┘    └───────────┘

WHY A SEPARATE CONSUMER?
------------------------
We could calculate features in the same consumer that writes raw_events.
But having a separate consumer gives us:
1. Independent scaling (features might be slower than raw writes)
2. Features can be recalculated by replaying Kafka
3. Clear separation of concerns

CONSUMER GROUP:
---------------
This consumer uses group_id "feature-calculator" (different from raw-events-writer).
This means BOTH consumers receive ALL messages (they're independent).

================================================================================
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

from kafka import KafkaConsumer

from db import connect_with_retry, upsert_features, ensure_features_table_exists
from state import StateStore
from features import FeatureCalculator


def setup_logger(level: str) -> None:
    """Configure logging to stdout with timestamp."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


def parse_iso(ts: str) -> datetime:
    """Parse ISO format timestamp string to datetime."""
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class GracefulKiller:
    """
    Handle shutdown signals (Ctrl+C, Docker stop) gracefully.
    
    When signal received, sets stop=True so main loop can exit cleanly.
    """
    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_):
        logging.getLogger("feature_consumer").info("Shutdown signal received")
        self.stop = True


def build_txn_data(msg, event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Kafka message to transaction data for feature calculation.
    
    Args:
        msg: Kafka message object
        event: Parsed JSON from message value
    
    Returns:
        Dictionary with transaction data
    """
    return {
        "transaction_id": event["transaction_id"],
        "user_id": event["user_id"],
        "card_id": event["card_id"],
        "merchant_id": event["merchant_id"],
        "amount": event["amount"],
        "currency": event["currency"],
        "event_time": parse_iso(event["timestamp"]),
        "channel": event["channel"],
        "country": event["country"],
        "city": event.get("city"),
        "device_id": event.get("device_id"),
        "ip_hash": event.get("ip_hash"),
        "label": event.get("label"),
    }


def main() -> None:
    """Main entry point for the feature consumer."""
    
    # =========================================================================
    # CONFIGURATION
    # =========================================================================
    setup_logger(os.getenv("LOG_LEVEL", "INFO"))
    log = logging.getLogger("feature_consumer")
    
    kafka_brokers = os.getenv("KAFKA_BROKERS", "redpanda:9092")
    topic = os.getenv("KAFKA_TOPIC", "transactions")
    
    # IMPORTANT: Different group_id from raw-events-writer!
    # This ensures both consumers receive all messages independently.
    group_id = os.getenv("KAFKA_GROUP_ID", "feature-calculator")
    
    auto_offset_reset = os.getenv("AUTO_OFFSET_RESET", "earliest")
    commit_every_n = int(os.getenv("COMMIT_EVERY_N", "50"))
    
    # =========================================================================
    # CONNECT TO DATABASE
    # =========================================================================
    log.info("Connecting to PostgreSQL...")
    conn = connect_with_retry()
    
    # Verify table exists
    ensure_features_table_exists(conn)
    conn.commit()
    
    # =========================================================================
    # INITIALIZE STATE AND CALCULATOR
    # =========================================================================
    log.info("Initializing state store and feature calculator...")
    state_store = StateStore(max_users=100_000)
    calculator = FeatureCalculator()
    
    # =========================================================================
    # CONNECT TO KAFKA
    # =========================================================================
    log.info("Connecting to Kafka: brokers=%s topic=%s group=%s", 
             kafka_brokers, topic, group_id)
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[b.strip() for b in kafka_brokers.split(",") if b.strip()],
        group_id=group_id,
        enable_auto_commit=False,  # Manual commit after DB write
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,  # Return from poll after 1 second if no messages
        max_poll_records=500,
    )
    
    log.info("Feature consumer started successfully!")
    
    # =========================================================================
    # MAIN PROCESSING LOOP
    # =========================================================================
    killer = GracefulKiller()
    processed = 0
    errors = 0
    last_log_time = time.time()
    
    try:
        while not killer.stop:
            # Poll for messages (returns after consumer_timeout_ms if none)
            for msg in consumer:
                if killer.stop:
                    break
                
                try:
                    event = msg.value
                    txn_data = build_txn_data(msg, event)
                    
                    # ==========================================================
                    # STEP 1: Get user state BEFORE adding this transaction
                    # ==========================================================
                    user_state = state_store.get_or_create_user(txn_data["user_id"])
                    
                    # ==========================================================
                    # STEP 2: Calculate features based on historical state
                    # ==========================================================
                    features = calculator.calculate(txn_data, user_state)
                    
                    # ==========================================================
                    # STEP 3: Add transaction to user's history
                    # (AFTER calculating features, so we don't count this txn)
                    # ==========================================================
                    state_store.add_transaction(txn_data["user_id"], txn_data)
                    
                    # ==========================================================
                    # STEP 4: Save features to database
                    # ==========================================================
                    upsert_features(conn, features)
                    conn.commit()
                    
                    processed += 1
                    
                    # ==========================================================
                    # STEP 5: Commit Kafka offset periodically
                    # ==========================================================
                    if processed % commit_every_n == 0:
                        consumer.commit()
                        
                        # Log progress
                        now = time.time()
                        if now - last_log_time >= 5:
                            log.info(
                                "Processed=%d errors=%d users_tracked=%d | "
                                "latest: txn=%s user=%s amount=%.2f zscore=%s",
                                processed, errors, state_store.get_user_count(),
                                features.get("transaction_id", "?")[:8],
                                features.get("user_id", "?"),
                                features.get("amount", 0),
                                features.get("amount_zscore", "N/A")
                            )
                            last_log_time = now
                
                except Exception as e:
                    errors += 1
                    conn.rollback()
                    log.exception("Error processing message: %s", e)
                    
                    # Don't commit offset for failed messages
                    # (they'll be reprocessed on restart)
                    
                    # If too many errors, something is seriously wrong
                    if errors > 100:
                        log.error("Too many errors, stopping consumer")
                        killer.stop = True
                        break
    
    finally:
        # =====================================================================
        # CLEANUP
        # =====================================================================
        log.info("Shutting down feature consumer...")
        
        try:
            consumer.commit()
        except Exception:
            pass
        
        try:
            consumer.close(timeout=10)
        except Exception:
            pass
        
        try:
            conn.close()
        except Exception:
            pass
        
        log.info(
            "Feature consumer stopped. Processed=%d errors=%d", 
            processed, errors
        )


if __name__ == "__main__":
    main()
