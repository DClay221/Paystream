# PayStream

A dual-mode payment data pipeline that processes simulated transaction events through a real-time fraud detection engine and a nightly batch settlement reporting system. Built to demonstrate production-grade data engineering architecture using Python, Apache Kafka, Apache Airflow, Docker, and AWS.

---

## Overview

PayStream simulates the core data engineering challenges faced by a payment processor. The system ingests transaction events, routes them through two cooperating pipelines — one optimized for low-latency fraud detection, the other for end-of-day merchant settlement reporting — and surfaces analytics via a cloud-native AWS stack.

The project is organized around a concrete business scenario: a payment processor needs to flag suspicious transactions the moment they occur while also producing accurate daily settlement summaries for each merchant. This dual requirement justifies two separate pipeline architectures co-existing in the same system, a pattern common in production fintech environments.

---

## Architecture

```
Simulated Payment Events
        |
        v
  Apache Kafka (Docker)            <-- real-time streaming layer
        |
        v
  Python Consumer Service          <-- enrichment + fraud scoring
  (Docker container)
        |
   +----+---------------------+
   |                          |
   v                          v
PostgreSQL (Docker)        Amazon S3 (Data Lake)
(fraud flags,              (Hive-partitioned JSON,
 aggregates)                raw events)
                               |
                               v
                          AWS Glue Crawler
                               |
                               v
                          Amazon Athena
                               |
                               v
                    Apache Airflow (Docker)      <-- nightly batch orchestration
                               |
                               v
                    Settlement Reports (Parquet)
                               |
                               v
                    Amazon Redshift (optional)
```

> A full architecture diagram will be added in Phase 5.

---

## Tech Stack

| Technology | Role |
|---|---|
| Apache Kafka | Real-time event streaming broker |
| Apache Zookeeper | Kafka cluster coordinator |
| Python | Simulator, consumer, fraud engine, pipeline scripts |
| PostgreSQL | Operational store for transactions and fraud alerts |
| Apache Airflow | Batch pipeline orchestration and DAG scheduling |
| Amazon S3 | Raw event data lake, partitioned by date |
| AWS Glue | Schema crawling and Data Catalog management |
| Amazon Athena | Serverless SQL queries on S3 data |
| Amazon SNS | Fraud threshold alerts and pipeline failure notifications |
| Amazon Redshift | Settlement data warehouse (optional) |
| Docker / Docker Compose | Container orchestration for all local services |

---

## Project Structure

```
paystream/
  |-- docker-compose.yml          # Kafka, Zookeeper, PostgreSQL, Airflow
  |-- .env.example                # Environment variable template
  |-- .gitignore
  |-- README.md
  |-- simulator/
  |   |-- Dockerfile
  |   |-- simulator.py            # Transaction event generator
  |   |-- requirements.txt
  |-- consumer/
  |   |-- Dockerfile
  |   |-- consumer.py             # Kafka consumer + fraud engine + writers
  |   |-- fraud_rules.py          # Rule engine (isolated for testability)
  |   |-- db.py                   # PostgreSQL writer
  |   |-- requirements.txt
  |-- db/
  |   |-- init.sql                # PostgreSQL schema initialization
  |-- airflow/
  |   |-- dags/
  |   |   |-- settlement_dag.py   # Nightly batch DAG (Phase 4)
  |   |-- plugins/
  |-- aws/
  |   |-- glue_crawler_setup.py   # Glue crawler configuration (Phase 3)
  |   |-- athena_queries/
  |   |   |-- settlement_daily.sql
  |   |   |-- fraud_summary.sql
  |   |-- sns_config.py           # SNS topic setup and helpers (Phase 2)
  |-- tests/
  |   |-- test_fraud_rules.py
  |   |-- test_simulator.py
  |-- docs/
      |-- architecture_diagram.png
```

---

## Getting Started

### Prerequisites

- Docker Desktop installed and running
- An AWS account (free tier is sufficient)
- Git

### Installation

Clone the repository:

```bash
git clone https://github.com/DClay221/Paystream.git
cd Paystream
```

Create your environment file from the template:

```bash
cp .env.example .env
```

Open `.env` and configure the following values:

```
POSTGRES_PASSWORD=your_password_here
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=paystream-lake
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:paystream-alerts
```

### Running the Pipeline

Build and start all services:

```bash
docker compose up --build
```

To run in detached mode:

```bash
docker compose up --build -d
```

To stop all services and remove containers:

```bash
docker compose down
```

To stop and remove all containers including the PostgreSQL data volume:

```bash
docker compose down -v
```

### Verifying the Pipeline

Once running, connect to PostgreSQL to confirm data is flowing:

```bash
docker exec -it paystream-postgres psql -U paystream -d paystream_db
```

Then run:

```sql
-- Total transactions processed
SELECT COUNT(*) FROM transactions;

-- Breakdown by status
SELECT status, COUNT(*)
FROM transactions
GROUP BY status
ORDER BY count DESC;

-- Fraud alerts with rule details
SELECT merchant_name, amount_usd, fraud_score, rule_triggered
FROM v_fraud_summary
LIMIT 10;

-- Daily merchant settlement summary
SELECT * FROM v_merchant_daily_summary LIMIT 5;
```

---

## Pipeline Overview

### Streaming Path (Real-Time)

The streaming path handles low-latency fraud detection and operates continuously.

The transaction simulator publishes events to the Kafka topic `payments.raw` at a configurable rate (default: 10 events per second). A Python consumer service subscribes to the topic, enriches each event with merchant and card metadata, and applies a rule-based fraud scoring engine. Events scoring above the configured threshold (default: 0.65) are flagged and written to the `fraud_alerts` table in PostgreSQL. An SNS notification is dispatched for every flagged transaction. All events, flagged or not, are simultaneously written to Amazon S3 in Hive-partitioned JSON format.

### Batch Path (Nightly)

The batch path handles end-of-day aggregation and reporting and runs on a schedule via Apache Airflow.

A DAG runs nightly and executes the following tasks in sequence: validate the day's S3 partition, trigger the Glue crawler to refresh the schema catalog, run a settlement aggregation query via Athena, write the results to S3 as a Parquet file, and optionally load the settlement data into Amazon Redshift for warehousing.

---

## Fraud Detection Rules

The consumer applies a configurable rule engine to each incoming transaction. Rules are additive — each contributes a weighted score, and the total determines the `fraud_score` field. A transaction is flagged when the score exceeds the threshold (default: 0.65).

| Rule | Trigger Condition | Score Weight |
|---|---|---|
| HIGH_VELOCITY_SAME_IP | 5 or more transactions from the same IP within 60 seconds | 0.40 |
| HIGH_VELOCITY_SAME_CARD | 3 or more transactions on the same card within 30 seconds | 0.45 |
| UNUSUAL_AMOUNT | Amount more than 3 standard deviations above merchant historical average | 0.25 |
| GEOGRAPHIC_MISMATCH | Card BIN country does not match merchant location | 0.20 |
| HIGH_RISK_MCC | Merchant category code is in the configured high-risk list | 0.15 |
| DECLINED_THEN_RETRY | Same card declined 2 or more times then approved within 5 minutes | 0.35 |
| OFF_HOURS_LARGE | Amount exceeds $500 and timestamp falls between 01:00 and 05:00 UTC | 0.20 |

---

## Environment Variables

All configuration is managed through environment variables. Copy `.env.example` to `.env` and populate the values before running the pipeline. Never commit your `.env` file — it is included in `.gitignore` by default.

| Variable | Description | Default |
|---|---|---|
| POSTGRES_USER | PostgreSQL username | paystream |
| POSTGRES_PASSWORD | PostgreSQL password | — |
| POSTGRES_DB | PostgreSQL database name | paystream_db |
| EVENTS_PER_SECOND | Simulator event production rate | 10 |
| FRAUD_INJECTION_RATE | Fraction of events seeded with suspicious patterns | 0.05 |
| FRAUD_SCORE_THRESHOLD | Minimum score required to flag a transaction | 0.65 |
| AWS_ACCESS_KEY_ID | AWS credentials | — |
| AWS_SECRET_ACCESS_KEY | AWS credentials | — |
| AWS_DEFAULT_REGION | AWS region | us-east-1 |
| S3_BUCKET | S3 bucket name for the data lake | paystream-lake |
| SNS_TOPIC_ARN | ARN of the SNS topic for alerts | — |

---

## Roadmap

- [x] Phase 1 — Streaming pipeline with Kafka, PostgreSQL, and fraud detection engine
- [ ] Phase 2 — S3 data lake integration and SNS alerting
- [ ] Phase 3 — AWS Glue crawler and Athena serverless SQL
- [ ] Phase 4 — Airflow orchestration and nightly settlement DAG
- [ ] Phase 5 — Documentation polish, architecture diagram, and demo recording

---

## Author

**Devyn Claybrooks**
GitHub: [DClay221](https://github.com/DClay221)