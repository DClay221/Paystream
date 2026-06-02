"""
PayStream — sns_config.py
Handles SNS notification publishing for fraud alerts
and pipeline failure events.
"""

import logging
import json
import os
import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

SNS_TOPIC_ARN     = os.getenv("SNS_TOPIC_ARN")
AWS_REGION        = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


# ─────────────────────────────────────────────
# SNS client
# ─────────────────────────────────────────────
def get_sns_client():
    return boto3.client("sns", region_name=AWS_REGION)


# ─────────────────────────────────────────────
# Notification publishers
# ─────────────────────────────────────────────
def publish_fraud_alert(event: dict, rules_fired: list[str]) -> bool:
    """
    Publishes an SNS notification for a flagged transaction.
    Returns True if successful, False if the publish failed.
    """
    if not SNS_TOPIC_ARN:
        log.warning("SNS_TOPIC_ARN not set — skipping fraud alert notification")
        return False

    subject = "PayStream Fraud Alert — Transaction Flagged"

    message = {
        "alert_type":     "FRAUD_DETECTED",
        "transaction_id": event.get("transaction_id"),
        "merchant_name":  event.get("merchant_name"),
        "merchant_id":    event.get("merchant_id"),
        "amount_usd":     event.get("amount_usd"),
        "card_network":   event.get("card_network"),
        "fraud_score":    event.get("fraud_score"),
        "rules_fired":    rules_fired,
        "timestamp":      event.get("timestamp"),
        "status":         event.get("status"),
    }

    return _publish(subject, message)


def publish_pipeline_failure(component: str, error: str) -> bool:
    """
    Publishes an SNS notification when a pipeline component fails.
    Called from Airflow DAG on_failure_callback in Phase 4.
    Returns True if successful, False if the publish failed.
    """
    if not SNS_TOPIC_ARN:
        log.warning("SNS_TOPIC_ARN not set — skipping pipeline failure notification")
        return False

    subject = f"PayStream Pipeline Failure — {component}"

    message = {
        "alert_type": "PIPELINE_FAILURE",
        "component":  component,
        "error":      error,
    }

    return _publish(subject, message)


# ─────────────────────────────────────────────
# Internal publish helper
# ─────────────────────────────────────────────
def _publish(subject: str, message: dict) -> bool:
    """
    Publishes a message to the configured SNS topic.
    Returns True on success, False on failure.
    """
    try:
        client = get_sns_client()
        response = client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject limit is 100 characters
            Message=json.dumps(message, indent=2),
        )
        log.debug("SNS message published — MessageId: %s", response["MessageId"])
        return True
    except ClientError as e:
        log.error("SNS publish failed: %s", e.response["Error"]["Message"])
        return False
    except Exception as e:
        log.error("SNS publish failed with unexpected error: %s", e)
        return False