"""
PayStream — consumer.py
Kafka consumer that:
  1. Reads transaction events from the 'payments.raw' topic
  2. Runs the fraud rule engine on each event
  3. Writes every event to PostgreSQL (transactions table)
  4. Writes flagged events to PostgreSQL (fraud_alerts table)
  5. Buffers events and flushes to S3 in Hive-partitioned JSON batches
  6. Sends SNS notifications for fraud alerts (Phase 2 — stubbed here)

Phase 1 scope: steps 1-4 only (S3 and SNS wired in Phase 2).
"""

import json
import logging
import os
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import db
import fraud_rules

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config from environment
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "payments.raw")
KAFKA_GROUP_ID          = "paystream-consumer-group"

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER     = os.getenv("POSTGRES_USER", "paystream")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "paystream_db")

FRAUD_SCORE_THRESHOLD = float(os.getenv("FRAUD_SCORE_THRESHOLD", 0.65))


# ─────────────────────────────────────────────
# Kafka consumer setup with retry logic
# ─────────────────────────────────────────────
def create_consumer(retries: int = 10, delay: int = 5) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            log.info("Connected to Kafka at %s — subscribed to %s",
                     KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
            return consumer
        except NoBrokersAvailable:
            log.warning(
                "Kafka not ready (attempt %d/%d) — retrying in %ds",
                attempt, retries, delay
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after %d attempts" % retries)


# ─────────────────────────────────────────────
# Database connection with retry logic
# ─────────────────────────────────────────────
def create_db_connection(retries: int = 10, delay: int = 5):
    for attempt in range(1, retries + 1):
        try:
            conn = db.get_connection(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB,
            )
            log.info("Connected to PostgreSQL at %s:%d/%s",
                     POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
            return conn
        except Exception as e:
            log.warning(
                "PostgreSQL not ready (attempt %d/%d): %s — retrying in %ds",
                attempt, retries, e, delay
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL after %d attempts" % retries)

def wait_for_schema(conn, retries: int = 20, delay: int = 5) -> None:
    """Waits until the transactions table exists before consuming."""
    for attempt in range(1, retries + 1):
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM transactions LIMIT 1;")
            log.info("Schema ready — starting consumption")
            return
        except Exception:
            conn.rollback()
            log.warning("Schema not ready (attempt %d/%d) — retrying in %ds",
                        attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Schema never became ready")

# ─────────────────────────────────────────────
# Event processing
# ─────────────────────────────────────────────
def process_event(event: dict, conn) -> None:
    """
    Runs the fraud engine on a single event, updates the event
    dict with fraud_score and fraud_flag, then persists to
    PostgreSQL. Logs a summary line for every flagged event.
    """
    # Run fraud rules
    score, rules_fired = fraud_rules.evaluate_transaction(event, conn)
    event["fraud_score"] = score
    event["fraud_flag"]  = score >= FRAUD_SCORE_THRESHOLD

    # Write transaction to PostgreSQL
    db.insert_transaction(conn, event)

    # Handle fraud flag
    if event["fraud_flag"]:
        for rule in rules_fired:
            db.insert_fraud_alert(conn, event["transaction_id"], rule, score)

        log.warning(
            "FRAUD FLAGGED | txn=%s | merchant=%s | $%.2f | score=%.3f | rules=%s",
            event["transaction_id"][:8],
            event["merchant_name"],
            event["amount_usd"],
            score,
            ", ".join(rules_fired),
        )


# ─────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────
def main():
    log.info("Starting consumer — fraud threshold=%.2f", FRAUD_SCORE_THRESHOLD)

    consumer = create_consumer()
    conn     = create_db_connection()
    wait_for_schema(conn)

    events_processed = 0
    fraud_count      = 0

    try:
        for message in consumer:
            event = message.value

            try:
                process_event(event, conn)
                events_processed += 1

                if event["fraud_flag"]:
                    fraud_count += 1

                if events_processed % 100 == 0:
                    log.info(
                        "Processed %d events | %d fraud flags (%.1f%%)",
                        events_processed,
                        fraud_count,
                        (fraud_count / events_processed) * 100,
                    )

            except Exception as e:
                log.error("Failed to process event %s: %s",
                          event.get("transaction_id", "unknown"), e)
                # Reconnect to PostgreSQL if connection was lost
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass
                conn = create_db_connection()

    except KeyboardInterrupt:
        log.info("Consumer stopped — %d total events, %d fraud flags",
                 events_processed, fraud_count)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()