# ─────────────────────────────────────────────
# PayStream — Simulator container
# ─────────────────────────────────────────────
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first (better Docker layer caching —
# only re-runs if requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy simulator source
COPY simulator.py .

# Run the simulator
CMD ["python", "simulator.py"]