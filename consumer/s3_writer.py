"""
PayStream — s3_writer.py
Buffers transaction events and flushes them to Amazon S3
in Hive-partitioned JSON format for Glue/Athena compatibility.

Partition structure:
    s3://{bucket}/raw/year=YYYY/month=MM/day=DD/{filename}.json

Events are buffered in memory and flushed either when the buffer
reaches FLUSH_BATCH_SIZE or when flush() is called explicitly
(e.g. on consumer shutdown).
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
S3_BUCKET       = os.getenv("S3_BUCKET", "paystream-lake")
AWS_REGION      = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
FLUSH_BATCH_SIZE = int(os.getenv("S3_FLUSH_BATCH_SIZE", 100))


# ─────────────────────────────────────────────
# S3Writer class
# ─────────────────────────────────────────────
class S3Writer:
    """
    Buffers events by partition key and flushes to S3 in batches.
    Each flush writes one JSON file per partition containing all
    buffered events for that date partition.
    """

    def __init__(self):
        self.client  = boto3.client("s3", region_name=AWS_REGION)
        self.buffer  = defaultdict(list)  # partition_key -> [events]
        self.total_flushed = 0

    def add_event(self, event: dict) -> None:
        """
        Adds an event to the in-memory buffer.
        Triggers a flush if the total buffer size hits FLUSH_BATCH_SIZE.
        """
        partition_key = self._get_partition_key(event)
        self.buffer[partition_key].append(event)

        total_buffered = sum(len(v) for v in self.buffer.values())
        if total_buffered >= FLUSH_BATCH_SIZE:
            self.flush()

    def flush(self) -> None:
        """
        Writes all buffered events to S3, one file per partition.
        Clears the buffer after a successful write.
        """
        if not self.buffer:
            return

        for partition_key, events in self.buffer.items():
            if not events:
                continue
            try:
                self._write_partition(partition_key, events)
                self.total_flushed += len(events)
                log.info(
                    "Flushed %d events to S3 partition %s (total flushed: %d)",
                    len(events), partition_key, self.total_flushed
                )
            except Exception as e:
                log.error("Failed to flush partition %s to S3: %s", partition_key, e)

        self.buffer.clear()

    # ─────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────
    def _get_partition_key(self, event: dict) -> str:
        """
        Derives the Hive-style partition path from the event timestamp.
        Falls back to current UTC time if timestamp is missing or unparseable.
        """
        try:
            ts = datetime.fromisoformat(event["timestamp"])
        except (KeyError, ValueError, TypeError):
            ts = datetime.now(timezone.utc)

        return f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}"

    def _write_partition(self, partition_key: str, events: list) -> None:
        """
        Serializes events to newline-delimited JSON and writes to S3.
        Each file is uniquely named with a UUID to avoid overwrites
        when multiple consumer instances run concurrently.
        """
        s3_key = f"raw/{partition_key}/events_{uuid.uuid4().hex}.json"

        # Newline-delimited JSON (one event per line) — standard for
        # big data tools including Glue, Athena, and Spark
        body = "\n".join(json.dumps(event) for event in events)

        try:
            self.client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
        except ClientError as e:
            log.error(
                "S3 put_object failed for key %s: %s",
                s3_key,
                e.response["Error"]["Message"]
            )
            raise