"""
PayStream — simulator.py
Generates realistic payment transaction events and publishes
them to the Kafka topic 'payments.raw'.

Environment variables (set via docker-compose / .env):
    KAFKA_BOOTSTRAP_SERVERS  e.g. kafka:29092
    EVENTS_PER_SECOND        how many events to produce per second (default 10)
    FRAUD_INJECTION_RATE     fraction of events seeded as suspicious (default 0.05)
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SIMULATOR] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config from environment
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = "payments.raw"
EVENTS_PER_SECOND       = float(os.getenv("EVENTS_PER_SECOND", 10))
FRAUD_INJECTION_RATE    = float(os.getenv("FRAUD_INJECTION_RATE", 0.05))
SLEEP_INTERVAL          = 1.0 / EVENTS_PER_SECOND

# ─────────────────────────────────────────────
# Static reference data
# Must stay in sync with merchant_master seed
# data in db/init.sql
# ─────────────────────────────────────────────
MERCHANTS = [
    {"id": "MERCH_001", "name": "Fresh Market Grocery",    "mcc": "5411", "state": "OH", "high_risk": False, "avg": 62.50,  "std": 18.00},
    {"id": "MERCH_002", "name": "QuickStop Gas & Go",      "mcc": "5541", "state": "OH", "high_risk": False, "avg": 45.00,  "std": 12.00},
    {"id": "MERCH_003", "name": "Downtown Diner",          "mcc": "5812", "state": "NY", "high_risk": False, "avg": 28.00,  "std": 9.00},
    {"id": "MERCH_004", "name": "TechZone Electronics",    "mcc": "5734", "state": "CA", "high_risk": False, "avg": 210.00, "std": 95.00},
    {"id": "MERCH_005", "name": "City Pharmacy",           "mcc": "5912", "state": "IL", "high_risk": False, "avg": 38.00,  "std": 14.00},
    {"id": "MERCH_006", "name": "FastShip eCommerce",      "mcc": "5999", "state": "TX", "high_risk": False, "avg": 75.00,  "std": 40.00},
    {"id": "MERCH_007", "name": "Grand Hotel & Suites",    "mcc": "7011", "state": "NV", "high_risk": False, "avg": 189.00, "std": 75.00},
    {"id": "MERCH_008", "name": "AutoParts Depot",         "mcc": "5533", "state": "MI", "high_risk": False, "avg": 95.00,  "std": 45.00},
    {"id": "MERCH_009", "name": "CryptoExchange Pro",      "mcc": "6051", "state": "FL", "high_risk": True,  "avg": 500.00, "std": 300.00},
    {"id": "MERCH_010", "name": "Global Wire Services",    "mcc": "4829", "state": "NY", "high_risk": True,  "avg": 750.00, "std": 400.00},
    {"id": "MERCH_011", "name": "Sunset Clothing Co",      "mcc": "5621", "state": "CA", "high_risk": False, "avg": 55.00,  "std": 25.00},
    {"id": "MERCH_012", "name": "Metro Bookstore",         "mcc": "5942", "state": "MA", "high_risk": False, "avg": 22.00,  "std": 8.00},
    {"id": "MERCH_013", "name": "Riverside Gym & Fitness", "mcc": "7941", "state": "CO", "high_risk": False, "avg": 49.00,  "std": 10.00},
    {"id": "MERCH_014", "name": "Lucky Star Casino",       "mcc": "7995", "state": "NV", "high_risk": True,  "avg": 300.00, "std": 250.00},
    {"id": "MERCH_015", "name": "Lakeside Pet Supply",     "mcc": "5995", "state": "WA", "high_risk": False, "avg": 42.00,  "std": 18.00},
]

CARD_NETWORKS    = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]
TRANSACTION_TYPES = ["PURCHASE", "PURCHASE", "PURCHASE", "PURCHASE", "REFUND", "AUTH_ONLY"]
STATUSES_NORMAL  = ["APPROVED", "APPROVED", "APPROVED", "DECLINED"]
DECLINE_REASONS  = ["INSUFFICIENT_FUNDS", "FRAUD_SUSPECTED", "CARD_EXPIRED", "DO_NOT_HONOR"]

# High-risk MCCs — must stay in sync with fraud_rules.py
HIGH_RISK_MCCS = {"6051", "4829", "7995", "7801", "7802"}


# ─────────────────────────────────────────────
# Kafka producer setup with retry logic
# ─────────────────────────────────────────────
def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """
    Attempts to connect to Kafka with retries.
    Kafka takes a few seconds to be ready after container start
    even after the healthcheck passes.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",           # wait for broker acknowledgement
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning(
                "Kafka not ready (attempt %d/%d) — retrying in %ds",
                attempt, retries, delay
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after %d attempts" % retries)


# ─────────────────────────────────────────────
# Event generation helpers
# ─────────────────────────────────────────────
fake = Faker()

def generate_normal_event() -> dict:
    """Generates a realistic but unremarkable payment transaction."""
    merchant  = random.choice(MERCHANTS)
    tx_type   = random.choice(TRANSACTION_TYPES)
    status    = random.choice(STATUSES_NORMAL)
    amount    = round(max(1.00, random.gauss(merchant["avg"], merchant["std"])), 2)

    return {
        "transaction_id":         str(uuid.uuid4()),
        "timestamp":              datetime.now(timezone.utc).isoformat(),
        "merchant_id":            merchant["id"],
        "merchant_name":          merchant["name"],
        "merchant_category_code": merchant["mcc"],
        "merchant_state":         merchant["state"],
        "card_bin":               str(random.randint(400000, 699999)),
        "card_last4":             str(random.randint(1000, 9999)),
        "card_network":           random.choice(CARD_NETWORKS),
        "amount_usd":             amount,
        "currency":               "USD",
        "transaction_type":       tx_type,
        "status":                 status,
        "decline_reason":         random.choice(DECLINE_REASONS) if status == "DECLINED" else None,
        "device_fingerprint":     fake.sha256()[:32],
        "ip_address":             fake.ipv4(),
        "is_card_present":        random.choice([True, False]),
        "fraud_score":            0.0,   # assigned by consumer fraud engine
        "fraud_flag":             False, # assigned by consumer fraud engine
    }


def generate_fraud_event() -> dict:
    """
    Generates a transaction seeded with suspicious characteristics.
    One of several fraud patterns is randomly chosen so the rule
    engine has varied signals to detect.
    """
    event   = generate_normal_event()
    pattern = random.choice([
        "high_velocity_ip",
        "high_velocity_card",
        "unusual_amount",
        "geographic_mismatch",
        "high_risk_merchant",
        "off_hours_large",
    ])

    if pattern == "high_velocity_ip":
        # Reuse the same IP to trip the velocity rule
        event["ip_address"] = "192.168.1.99"

    elif pattern == "high_velocity_card":
        # Reuse same card details
        event["card_bin"]   = "411111"
        event["card_last4"] = "0000"

    elif pattern == "unusual_amount":
        # Spike the amount well above merchant average
        merchant = next(m for m in MERCHANTS if m["id"] == event["merchant_id"])
        event["amount_usd"] = round(merchant["avg"] + (merchant["std"] * 5), 2)

    elif pattern == "geographic_mismatch":
        # International-looking BIN (non-US range)
        event["card_bin"] = str(random.randint(300000, 369999))  # Diners/international range

    elif pattern == "high_risk_merchant":
        # Force a high-risk merchant
        hr_merchant = random.choice([m for m in MERCHANTS if m["high_risk"]])
        event["merchant_id"]            = hr_merchant["id"]
        event["merchant_name"]          = hr_merchant["name"]
        event["merchant_category_code"] = hr_merchant["mcc"]
        event["merchant_state"]         = hr_merchant["state"]
        event["amount_usd"]             = round(
            max(1.00, random.gauss(hr_merchant["avg"], hr_merchant["std"])), 2
        )

    elif pattern == "off_hours_large":
        # Large amount, override timestamp to off-hours UTC
        event["amount_usd"] = round(random.uniform(500, 2000), 2)
        off_hour = random.randint(1, 4)
        ts = datetime.now(timezone.utc).replace(hour=off_hour, minute=random.randint(0, 59))
        event["timestamp"] = ts.isoformat()

    return event


# ─────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────
def main():
    log.info(
        "Starting simulator — %.1f events/sec, %.0f%% fraud injection rate",
        EVENTS_PER_SECOND,
        FRAUD_INJECTION_RATE * 100,
    )

    producer = create_producer()
    events_sent = 0

    try:
        while True:
            is_fraud_seed = random.random() < FRAUD_INJECTION_RATE
            event = generate_fraud_event() if is_fraud_seed else generate_normal_event()

            producer.send(KAFKA_TOPIC, value=event)
            events_sent += 1

            if events_sent % 100 == 0:
                log.info(
                    "Published %d events (last: %s | $%.2f | %s)",
                    events_sent,
                    event["merchant_name"],
                    event["amount_usd"],
                    "FRAUD_SEED" if is_fraud_seed else "normal",
                )

            time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
        log.info("Simulator stopped — %d total events published", events_sent)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()