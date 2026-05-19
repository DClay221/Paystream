"""
PayStream — db.py
Handles all PostgreSQL read/write operations for the consumer.
Kept separate from consumer.py so the fraud engine and DB logic
can be tested independently.
"""

import logging
import uuid
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────
def get_connection(host: str, port: int, user: str, password: str, dbname: str):
    """Creates and returns a psycopg2 connection."""
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )


# ─────────────────────────────────────────────
# Transactions
# ─────────────────────────────────────────────
def insert_transaction(conn, event: dict) -> None:
    """
    Inserts a single transaction event into the transactions table.
    Uses ON CONFLICT DO NOTHING so duplicate Kafka deliveries are safe.
    """
    sql = """
        INSERT INTO transactions (
            transaction_id, timestamp, merchant_id, merchant_name,
            merchant_category_code, merchant_state, card_bin, card_last4,
            card_network, amount_usd, currency, transaction_type, status,
            decline_reason, device_fingerprint, ip_address, is_card_present,
            fraud_score, fraud_flag
        ) VALUES (
            %(transaction_id)s, %(timestamp)s, %(merchant_id)s, %(merchant_name)s,
            %(merchant_category_code)s, %(merchant_state)s, %(card_bin)s, %(card_last4)s,
            %(card_network)s, %(amount_usd)s, %(currency)s, %(transaction_type)s, %(status)s,
            %(decline_reason)s, %(device_fingerprint)s, %(ip_address)s, %(is_card_present)s,
            %(fraud_score)s, %(fraud_flag)s
        )
        ON CONFLICT (transaction_id) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, event)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def insert_fraud_alert(conn, transaction_id: str, rule_triggered: str, fraud_score: float) -> None:
    """Inserts a fraud alert row for a flagged transaction."""
    sql = """
        INSERT INTO fraud_alerts (
            alert_id, transaction_id, alert_time, rule_triggered,
            fraud_score, sns_notification_sent
        ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (
                str(uuid.uuid4()),
                transaction_id,
                datetime.now(timezone.utc),
                rule_triggered,
                fraud_score,
                False,  # SNS sending is handled separately in consumer.py
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ─────────────────────────────────────────────
# Velocity tables — used by fraud rules
# ─────────────────────────────────────────────
def get_ip_transaction_count(conn, ip_address: str, window_seconds: int = 60) -> int:
    """
    Returns the number of transactions from the given IP
    within the last window_seconds seconds.
    """
    sql = """
        SELECT COUNT(*) FROM transactions
        WHERE ip_address = %s
          AND timestamp >= NOW() AT TIME ZONE 'UTC' - INTERVAL '%s seconds';
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (ip_address, window_seconds))
            return cur.fetchone()[0]
    except Exception:
        conn.rollback()
        raise


def get_card_transaction_count(conn, card_bin: str, card_last4: str, window_seconds: int = 30) -> int:
    """
    Returns the number of transactions for the given card
    within the last window_seconds seconds.
    """
    sql = """
        SELECT COUNT(*) FROM transactions
        WHERE card_bin = %s
          AND card_last4 = %s
          AND timestamp >= NOW() AT TIME ZONE 'UTC' - INTERVAL '%s seconds';
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (card_bin, card_last4, window_seconds))
            return cur.fetchone()[0]
    except Exception:
        conn.rollback()
        raise


def get_card_recent_declines(conn, card_bin: str, card_last4: str, window_seconds: int = 300) -> int:
    """
    Returns the number of DECLINED transactions for a card
    within the last window_seconds seconds.
    Used by the DECLINED_THEN_RETRY rule.
    """
    sql = """
        SELECT COUNT(*) FROM transactions
        WHERE card_bin = %s
          AND card_last4 = %s
          AND status = 'DECLINED'
          AND timestamp >= NOW() AT TIME ZONE 'UTC' - INTERVAL '%s seconds';
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (card_bin, card_last4, window_seconds))
            return cur.fetchone()[0]
    except Exception:
        conn.rollback()
        raise


def get_merchant_avg_amount(conn, merchant_id: str) -> tuple[float, float]:
    """
    Returns (avg_amount, stddev_amount) for the merchant
    from the merchant_master seed table.
    Falls back to (50.0, 20.0) if merchant not found.
    """
    sql = """
        SELECT avg_transaction_usd, stddev_transaction_usd
        FROM merchant_master
        WHERE merchant_id = %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (merchant_id,))
            row = cur.fetchone()
        if row:
            return float(row[0]), float(row[1])
        return 50.0, 20.0
    except Exception:
        conn.rollback()
        raise