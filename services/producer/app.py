"""
PRODUCER - Transaction Generator with Prometheus Metrics
=========================================================

Generates synthetic credit card transactions and publishes to Kafka.
Exposes metrics on port 9091 for Prometheus scraping.
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple
from threading import Thread

from faker import Faker
from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, Gauge, start_http_server


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

TRANSACTIONS_PRODUCED = Counter(
    'fraud_transactions_produced_total',
    'Total number of transactions produced to Kafka',
    ['channel', 'country']
)

TRANSACTIONS_WITH_FRAUD_LABEL = Counter(
    'fraud_transactions_with_fraud_label_total',
    'Transactions with fraud label=True',
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

PRODUCER_RATE = Gauge(
    'fraud_producer_rate_per_second',
    'Configured production rate per second'
)


@dataclass(frozen=True)
class Config:
    kafka_brokers: str
    kafka_topic: str
    rate_per_sec: float
    users: int
    merchants: int
    cards_per_user: int
    random_seed: int
    log_level: str
    metrics_port: int


def load_config() -> Config:
    return Config(
        kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:19092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "transactions"),
        rate_per_sec=float(os.getenv("RATE_PER_SEC", "10")),
        users=int(os.getenv("USERS", "2000")),
        merchants=int(os.getenv("MERCHANTS", "250")),
        cards_per_user=int(os.getenv("CARDS_PER_USER", "2")),
        random_seed=int(os.getenv("RANDOM_SEED", "42")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        metrics_port=int(os.getenv("METRICS_PORT", "9091")),
    )


def setup_logger(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds")


def stable_ids(prefix: str, n: int) -> Tuple[str, ...]:
    return tuple(f"{prefix}{i:06d}" for i in range(1, n + 1))


def choose_weighted(rng: random.Random, items_with_weights):
    r = rng.random()
    upto = 0.0
    for item, w in items_with_weights:
        upto += w
        if r <= upto:
            return item
    return items_with_weights[-1][0]


def synthetic_label(event: Dict, rng: random.Random, user_home_country: str) -> Optional[bool]:
    amount = float(event["amount"])
    channel = event["channel"]
    country = event["country"]

    base = 0.002
    if amount >= 2000:
        base += 0.01
    if amount >= 5000:
        base += 0.03
    if channel == "ECOM" and country != user_home_country:
        base += 0.05
    if channel == "ATM" and amount >= 1000:
        base += 0.01

    return rng.random() < min(base, 0.25)


def build_event(
    fake: Faker,
    rng: random.Random,
    user_ids: Tuple[str, ...],
    merchant_ids: Tuple[str, ...],
    user_home: Dict[str, str],
    user_cards: Dict[str, Tuple[str, ...]],
) -> Dict:
    user_id = rng.choice(user_ids)
    card_id = rng.choice(user_cards[user_id])
    merchant_id = rng.choice(merchant_ids)

    channel = choose_weighted(
        rng,
        [("POS", 0.65), ("ECOM", 0.25), ("ATM", 0.10)],
    )

    currency = "USD"
    home_country = user_home[user_id]
    country = home_country if rng.random() < 0.92 else rng.choice(["US", "GB", "DE", "SG", "AE", "AU", "CA", "FR", "IN"])
    city = fake.city()

    amount = round((10 ** rng.uniform(0.6, 3.8)), 2)
    if channel == "ATM":
        amount = round(min(max(amount, 20), 1500), 2)
    if channel == "POS":
        amount = round(min(amount, 2500), 2)

    event_time = utc_now() - timedelta(milliseconds=rng.randint(0, 800))
    ingestion_time = utc_now()

    device_id = f"D{rng.randint(1, 50000):06d}" if channel != "POS" else None
    ip_hash = uuid.uuid4().hex[:16] if channel == "ECOM" else None

    evt = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "card_id": card_id,
        "merchant_id": merchant_id,
        "amount": amount,
        "currency": currency,
        "timestamp": iso(event_time),
        "ingestion_time": iso(ingestion_time),
        "channel": channel,
        "country": country,
        "city": city,
        "device_id": device_id,
        "ip_hash": ip_hash,
    }

    evt["label"] = synthetic_label(evt, rng, home_country)
    return evt


class GracefulKiller:
    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_):
        self.stop = True


def main() -> None:
    cfg = load_config()
    setup_logger(cfg.log_level)
    log = logging.getLogger("producer")

    # Start Prometheus metrics server
    log.info(f"Starting metrics server on port {cfg.metrics_port}")
    start_http_server(cfg.metrics_port)
    
    # Set rate gauge
    PRODUCER_RATE.set(cfg.rate_per_sec)

    rng = random.Random(cfg.random_seed)
    fake = Faker()
    Faker.seed(cfg.random_seed)

    user_ids = stable_ids("U", cfg.users)
    merchant_ids = stable_ids("M", cfg.merchants)

    home_countries = ["US", "GB", "DE", "FR", "AU", "CA", "SG", "AE", "IN"]
    user_home = {u: rng.choice(home_countries) for u in user_ids}
    user_cards = {u: tuple(f"C{u[1:]}_{i}" for i in range(1, cfg.cards_per_user + 1)) for u in user_ids}

    producer = KafkaProducer(
        bootstrap_servers=[b.strip() for b in cfg.kafka_brokers.split(",") if b.strip()],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        acks="all",
        linger_ms=10,
        retries=5,
    )

    killer = GracefulKiller()

    if cfg.rate_per_sec <= 0:
        raise ValueError("RATE_PER_SEC must be > 0")

    sleep_s = 1.0 / cfg.rate_per_sec
    sent = 0
    last_log = time.time()

    log.info(
        "Starting producer: brokers=%s topic=%s rate_per_sec=%.2f users=%d merchants=%d",
        cfg.kafka_brokers, cfg.kafka_topic, cfg.rate_per_sec, cfg.users, cfg.merchants
    )

    while not killer.stop:
        evt = build_event(fake, rng, user_ids, merchant_ids, user_home, user_cards)
        
        send_start = time.time()
        try:
            future = producer.send(cfg.kafka_topic, key=evt["user_id"], value=evt)
            future.get(timeout=10)
            
            # Record metrics
            send_latency = time.time() - send_start
            PRODUCER_LATENCY.observe(send_latency)
            TRANSACTIONS_PRODUCED.labels(channel=evt["channel"], country=evt["country"]).inc()
            
            if evt["label"]:
                TRANSACTIONS_WITH_FRAUD_LABEL.inc()
                
        except Exception as e:
            PRODUCER_ERRORS.labels(error_type=type(e).__name__).inc()
            log.error(f"Failed to send message: {e}")
            continue

        sent += 1

        now = time.time()
        if now - last_log >= 5:
            log.info("Sent %d events (latest txn_id=%s user=%s amount=%.2f label=%s)",
                     sent, evt["transaction_id"], evt["user_id"], float(evt["amount"]), evt["label"])
            last_log = now

        time.sleep(sleep_s)

    log.info("Stopping producer (flushing)...")
    producer.flush(timeout=10)
    producer.close(timeout=10)
    log.info("Producer stopped cleanly.")


if __name__ == "__main__":
    main()
