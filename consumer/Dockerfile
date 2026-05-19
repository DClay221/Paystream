# ─────────────────────────────────────────────
# PayStream — Consumer container
# ─────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY fraud_rules.py .
COPY db.py .

CMD ["python", "consumer.py"]