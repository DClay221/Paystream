"""
PayStream — fraud_rules.py
Rule-based fraud scoring engine.

Each rule function takes the current transaction event and a
database connection (for velocity lookups), and returns a
(score_contribution, rule_name) tuple if the rule fires,
or (0.0, None) if it does not.

The consumer calls evaluate_transaction() which runs all rules,
sums the contributions, and returns the final fraud_score
and a list of rules that fired.
"""

import logging
from datetime import datetime, timezone

import db

log = logging.getLogger(__name__)

# High-risk MCCs — must stay in sync with simulator.py
HIGH_RISK_MCCS = {"6051", "4829", "7995", "7801", "7802"}


# ─────────────────────────────────────────────
# Individual rules
# ─────────────────────────────────────────────

def rule_high_velocity_same_ip(event: dict, conn) -> tuple[float, str | None]:
    """
    Fires if 5 or more transactions have come from the same IP
    address within the last 60 seconds.
    Catches card testing and credential stuffing attacks.
    """
    count = db.get_ip_transaction_count(conn, event["ip_address"], window_seconds=60)
    if count >= 5:
        log.debug("HIGH_VELOCITY_SAME_IP fired — IP %s count=%d", event["ip_address"], count)
        return 0.40, "HIGH_VELOCITY_SAME_IP"
    return 0.0, None


def rule_high_velocity_same_card(event: dict, conn) -> tuple[float, str | None]:
    """
    Fires if 3 or more transactions have been attempted on the
    same card (BIN + last4) within the last 30 seconds.
    Catches rapid sequential authorization attempts.
    """
    count = db.get_card_transaction_count(
        conn, event["card_bin"], event["card_last4"], window_seconds=30
    )
    if count >= 3:
        log.debug("HIGH_VELOCITY_SAME_CARD fired — card %s...%s count=%d",
                  event["card_bin"], event["card_last4"], count)
        return 0.45, "HIGH_VELOCITY_SAME_CARD"
    return 0.0, None


def rule_unusual_amount(event: dict, conn) -> tuple[float, str | None]:
    """
    Fires if the transaction amount is more than 3 standard
    deviations above the merchant's historical average.
    Uses merchant_master seed data for the baseline.
    """
    avg, std = db.get_merchant_avg_amount(conn, event["merchant_id"])
    if std > 0 and event["amount_usd"] > avg + (3 * std):
        log.debug("UNUSUAL_AMOUNT fired — $%.2f vs avg $%.2f std $%.2f",
                  event["amount_usd"], avg, std)
        return 0.25, "UNUSUAL_AMOUNT"
    return 0.0, None


def rule_geographic_mismatch(event: dict, conn) -> tuple[float, str | None]:
    """
    Fires if the card BIN falls in an international range
    while the merchant is domestic.
    BINs 300000-369999 are used as a proxy for international cards
    in the simulator; real implementations would use a BIN database.
    """
    try:
        bin_int = int(event["card_bin"])
    except (ValueError, TypeError):
        return 0.0, None

    is_international_bin = 300000 <= bin_int <= 369999
    if is_international_bin:
        log.debug("GEOGRAPHIC_MISMATCH fired — BIN %s", event["card_bin"])
        return 0.20, "GEOGRAPHIC_MISMATCH"
    return 0.0, None


def rule_high_risk_mcc(event: dict, conn) -> tuple[float, str | None]:
    """
    Fires if the merchant's MCC is in the configured high-risk
    category list (crypto, wire transfer, gambling).
    """
    if event["merchant_category_code"] in HIGH_RISK_MCCS:
        log.debug("HIGH_RISK_MCC fired — MCC %s", event["merchant_category_code"])
        return 0.15, "HIGH_RISK_MCC"
    return 0.0, None


def rule_declined_then_retry(event: dict, conn) -> tuple[float, str | None]:
    """
    Fires if the same card has been declined 2 or more times in
    the last 5 minutes and the current transaction is APPROVED.
    Common pattern in carding attacks where fraudsters test cards
    until one goes through.
    """
    if event["status"] != "APPROVED":
        return 0.0, None
    decline_count = db.get_card_recent_declines(
        conn, event["card_bin"], event["card_last4"], window_seconds=300
    )
    if decline_count >= 2:
        log.debug("DECLINED_THEN_RETRY fired — card %s...%s declines=%d",
                  event["card_bin"], event["card_last4"], decline_count)
        return 0.35, "DECLINED_THEN_RETRY"
    return 0.0, None


def rule_off_hours_large(event: dict, conn) -> tuple[float, str | None]:
    """
    Fires if the transaction amount exceeds $500 AND the
    transaction timestamp falls between 01:00 and 05:00 UTC.
    Combines an amount signal with a time-of-day signal.
    """
    if event["amount_usd"] <= 500:
        return 0.0, None
    try:
        ts = datetime.fromisoformat(event["timestamp"])
        hour_utc = ts.astimezone(timezone.utc).hour
    except (ValueError, TypeError):
        return 0.0, None

    if 1 <= hour_utc <= 5:
        log.debug("OFF_HOURS_LARGE fired — $%.2f at hour %d UTC",
                  event["amount_usd"], hour_utc)
        return 0.20, "OFF_HOURS_LARGE"
    return 0.0, None


# ─────────────────────────────────────────────
# Rule registry — add new rules here
# ─────────────────────────────────────────────
RULES = [
    rule_high_velocity_same_ip,
    rule_high_velocity_same_card,
    rule_unusual_amount,
    rule_geographic_mismatch,
    rule_high_risk_mcc,
    rule_declined_then_retry,
    rule_off_hours_large,
]


# ─────────────────────────────────────────────
# Main evaluation function
# ─────────────────────────────────────────────
def evaluate_transaction(event: dict, conn) -> tuple[float, list[str]]:
    """
    Runs all rules against the transaction event.
    Returns (fraud_score, [list of rule names that fired]).

    fraud_score is capped at 1.0.
    The consumer uses this to set event["fraud_score"] and
    event["fraud_flag"] before writing to PostgreSQL and S3.
    """
    total_score  = 0.0
    rules_fired  = []

    for rule_fn in RULES:
        try:
            score, rule_name = rule_fn(event, conn)
            if score > 0.0 and rule_name:
                total_score += score
                rules_fired.append(rule_name)
        except Exception as e:
            log.warning("Rule %s raised an exception: %s", rule_fn.__name__, e)

    final_score = min(round(total_score, 3), 1.0)
    return final_score, rules_fired