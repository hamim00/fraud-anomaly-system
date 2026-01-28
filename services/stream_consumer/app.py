"""
STREAM CONSUMER - Raw Events Writer with Prometheus Metrics
============================================================

Consumes transactions from Kafka and writes to PostgreSQL raw_events table.
Exposes metrics on port 9092 for Prometheus scraping.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict
from threading import Thread

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, start_http_server

from db import connect_with_retry, insert_raw_event


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

MESSAGES_PROCESSED = Counter(
    'fraud_stream_consumer_messages_total',
    'Total messages processed by stream consumer',
    ['status']
)

CONSUMER_LAG = Gauge(
    'fraud_consumer_lag',
    'Consumer lag (messages behind)',
    ['consumer_group', 'partition']
)

PROCESSING_TIME = Histogram(
    'fraud_stream_consumer_processing_seconds',
    'Time to process a single message',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

DB_WRITE_LATENCY = Histogram(
    'fraud_stream_consumer_db_write_seconds',
    'Database write latency for raw_events',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

BATCH_SIZE = Gauge(
    'fraud_stream_consumer_batch_size',
    'Current batch size before commit'
)


def setup_logger(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


class GracefulKiller:
    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_):
        self.stop = True


def build_row(msg, event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kafka_topic": msg.topic,
        "kafka_partition": msg.partition,
        "kafka_offset": msg.offset,
        "transaction_id": event["transaction_id"],
        "user_id": event["user_id"],
        "card_id": event["card_id"],
        "merchant_id": event["merchant_id"],
        "amount": event["amount"],
        "currency": event["currency"],
        "event_time": parse_iso(event["timestamp"]),
        "ingestion_time": parse_iso(event["ingestion_time"]),
        "channel": event["channel"],
        "country": event["country"],
        "city": event.get("city"),
        "device_id": event.get("device_id"),
        "ip_hash": event.get("ip_hash"),
        "label": event.get("label"),
        "payload": psycopg2.extras.Json(event),
    }


def main() -> None:
    setup_logger(os.getenv("LOG_LEVEL", "INFO"))
    log = logging.getLogger("stream_consumer")

    # Start Prometheus metrics server
    metrics_port = int(os.getenv("METRICS_PORT", "9092"))
    log.info(f"Starting metrics server on port {metrics_port}")
    start_http_server(metrics_port)

    kafka_brokers = os.getenv("KAFKA_BROKERS", "redpanda:9092")
    topic = os.getenv("KAFKA_TOPIC", "transactions")
    group_id = os.getenv("KAFKA_GROUP_ID", "raw-events-writer")
    auto_offset_reset = os.getenv("AUTO_OFFSET_RESET", "earliest")
    batch_commit_every_n = int(os.getenv("BATCH_COMMIT_EVERY_N", "50"))

    conn = connect_with_retry()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[b.strip() for b in kafka_brokers.split(",") if b.strip()],
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
        max_poll_records=500,
    )

    log.info(
        "Consumer started: brokers=%s topic=%s group_id=%s auto_offset_reset=%s",
        kafka_brokers, topic, group_id, auto_offset_reset
    )

    killer = GracefulKiller()
    processed = 0
    batch_count = 0

    try:
        while not killer.stop:
            for msg in consumer:
                process_start = time.time()
                event = msg.value
                row = build_row(msg, event)

                try:
                    db_start = time.time()
                    insert_raw_event(conn, row)
                    conn.commit()
                    DB_WRITE_LATENCY.observe(time.time() - db_start)
                    
                    MESSAGES_PROCESSED.labels(status='success').inc()
                except Exception as e:
                    conn.rollback()
                    MESSAGES_PROCESSED.labels(status='error').inc()
                    log.exception("DB insert failed (will NOT commit Kafka offset). Error=%s", e)
                    break

                processed += 1
                batch_count += 1
                
                # Record processing time
                PROCESSING_TIME.observe(time.time() - process_start)
                BATCH_SIZE.set(batch_count)

                if processed % batch_commit_every_n == 0:
                    consumer.commit()
                    batch_count = 0
                    BATCH_SIZE.set(0)
                    
                    # Update lag metrics
                    try:
                        partitions = consumer.assignment()
                        for tp in partitions:
                            end_offsets = consumer.end_offsets([tp])
                            current_offset = consumer.position(tp)
                            lag = end_offsets[tp] - current_offset
                            CONSUMER_LAG.labels(
                                consumer_group=group_id,
                                partition=str(tp.partition)
                            ).set(lag)
                    except Exception:
                        pass
                    
                    log.info(
                        "Processed=%d committed_offsets (latest: partition=%s offset=%s txn_id=%s label=%s amount=%s)",
                        processed, msg.partition, msg.offset, event.get("transaction_id"), event.get("label"), event.get("amount")
                    )

    finally:
        log.info("Shutting down consumer...")
        try:
            consumer.commit()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        log.info("Consumer stopped cleanly.")


if __name__ == "__main__":
    main()
