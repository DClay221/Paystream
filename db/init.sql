-- =============================================================
-- PayStream — PostgreSQL Schema Initialization
-- Runs automatically when the postgres container first starts
-- =============================================================


-- =============================================================
-- TABLE: transactions
-- Stores every payment event consumed from Kafka
-- =============================================================
CREATE TABLE IF NOT EXISTS transactions (
    -- Identity
    transaction_id          UUID            PRIMARY KEY,
    timestamp               TIMESTAMPTZ     NOT NULL,

    -- Merchant
    merchant_id             VARCHAR(20)     NOT NULL,
    merchant_name           VARCHAR(100)    NOT NULL,
    merchant_category_code  CHAR(4)         NOT NULL,
    merchant_state          CHAR(2)         NOT NULL,

    -- Card
    card_bin                CHAR(6)         NOT NULL,
    card_last4              CHAR(4)         NOT NULL,
    card_network            VARCHAR(12)     NOT NULL,

    -- Transaction
    amount_usd              NUMERIC(12, 2)  NOT NULL,
    currency                CHAR(3)         NOT NULL DEFAULT 'USD',
    transaction_type        VARCHAR(20)     NOT NULL,
    status                  VARCHAR(20)     NOT NULL,
    decline_reason          VARCHAR(50)     NULL,

    -- Device / Network
    device_fingerprint      VARCHAR(64)     NOT NULL,
    ip_address              VARCHAR(45)     NOT NULL,
    is_card_present         BOOLEAN         NOT NULL,

    -- Fraud
    fraud_score             NUMERIC(4, 3)   NOT NULL DEFAULT 0.000,
    fraud_flag              BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Metadata
    ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_transactions_merchant_id
    ON transactions (merchant_id);

CREATE INDEX IF NOT EXISTS idx_transactions_timestamp
    ON transactions (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_fraud_flag
    ON transactions (fraud_flag)
    WHERE fraud_flag = TRUE;

CREATE INDEX IF NOT EXISTS idx_transactions_ip_address
    ON transactions (ip_address);

CREATE INDEX IF NOT EXISTS idx_transactions_card
    ON transactions (card_bin, card_last4);


-- =============================================================
-- TABLE: fraud_alerts
-- One row per fraud rule that fired on a transaction
-- A single transaction can trigger multiple rules
-- =============================================================
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id                UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id          UUID            NOT NULL REFERENCES transactions(transaction_id),
    alert_time              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    rule_triggered          VARCHAR(50)     NOT NULL,
    fraud_score             NUMERIC(4, 3)   NOT NULL,
    sns_notification_sent   BOOLEAN         NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_transaction_id
    ON fraud_alerts (transaction_id);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_rule
    ON fraud_alerts (rule_triggered);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_alert_time
    ON fraud_alerts (alert_time DESC);


-- =============================================================
-- TABLE: merchant_velocity
-- Rolling window aggregates used by the fraud rule engine
-- Updated by the consumer on every transaction
-- =============================================================
CREATE TABLE IF NOT EXISTS merchant_velocity (
    merchant_id             VARCHAR(20)     PRIMARY KEY,
    window_start            TIMESTAMPTZ     NOT NULL,
    transaction_count       INTEGER         NOT NULL DEFAULT 0,
    total_amount_usd        NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    avg_amount_usd          NUMERIC(12, 2)  NOT NULL DEFAULT 0.00,
    last_updated            TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


-- =============================================================
-- TABLE: ip_velocity
-- Tracks transaction counts per IP in a rolling 60-second window
-- Used by the HIGH_VELOCITY_SAME_IP fraud rule
-- =============================================================
CREATE TABLE IF NOT EXISTS ip_velocity (
    ip_address              VARCHAR(45)     NOT NULL,
    window_start            TIMESTAMPTZ     NOT NULL,
    transaction_count       INTEGER         NOT NULL DEFAULT 0,
    last_updated            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ip_address, window_start)
);

CREATE INDEX IF NOT EXISTS idx_ip_velocity_last_updated
    ON ip_velocity (last_updated DESC);


-- =============================================================
-- TABLE: card_velocity
-- Tracks transaction counts per card in a rolling 30-second window
-- Used by the HIGH_VELOCITY_SAME_CARD fraud rule
-- =============================================================
CREATE TABLE IF NOT EXISTS card_velocity (
    card_bin                CHAR(6)         NOT NULL,
    card_last4              CHAR(4)         NOT NULL,
    window_start            TIMESTAMPTZ     NOT NULL,
    transaction_count       INTEGER         NOT NULL DEFAULT 0,
    last_declined           TIMESTAMPTZ     NULL,
    decline_count           INTEGER         NOT NULL DEFAULT 0,
    last_updated            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (card_bin, card_last4, window_start)
);


-- =============================================================
-- SEED DATA: merchant_master
-- Static lookup table of simulated merchants
-- Referenced by the simulator and consumer for enrichment
-- =============================================================
CREATE TABLE IF NOT EXISTS merchant_master (
    merchant_id             VARCHAR(20)     PRIMARY KEY,
    merchant_name           VARCHAR(100)    NOT NULL,
    merchant_category_code  CHAR(4)         NOT NULL,
    merchant_state          CHAR(2)         NOT NULL,
    is_high_risk            BOOLEAN         NOT NULL DEFAULT FALSE,
    avg_transaction_usd     NUMERIC(10, 2)  NOT NULL DEFAULT 50.00,
    stddev_transaction_usd  NUMERIC(10, 2)  NOT NULL DEFAULT 20.00
);

INSERT INTO merchant_master VALUES
    ('MERCH_001', 'Fresh Market Grocery',       '5411', 'OH', FALSE, 62.50,  18.00),
    ('MERCH_002', 'QuickStop Gas & Go',         '5541', 'OH', FALSE, 45.00,  12.00),
    ('MERCH_003', 'Downtown Diner',             '5812', 'NY', FALSE, 28.00,   9.00),
    ('MERCH_004', 'TechZone Electronics',       '5734', 'CA', FALSE, 210.00, 95.00),
    ('MERCH_005', 'City Pharmacy',              '5912', 'IL', FALSE, 38.00,  14.00),
    ('MERCH_006', 'FastShip eCommerce',         '5999', 'TX', FALSE, 75.00,  40.00),
    ('MERCH_007', 'Grand Hotel & Suites',       '7011', 'NV', FALSE, 189.00, 75.00),
    ('MERCH_008', 'AutoParts Depot',            '5533', 'MI', FALSE, 95.00,  45.00),
    ('MERCH_009', 'CryptoExchange Pro',         '6051', 'FL', TRUE,  500.00, 300.00),
    ('MERCH_010', 'Global Wire Services',       '4829', 'NY', TRUE,  750.00, 400.00),
    ('MERCH_011', 'Sunset Clothing Co',         '5621', 'CA', FALSE, 55.00,  25.00),
    ('MERCH_012', 'Metro Bookstore',            '5942', 'MA', FALSE, 22.00,   8.00),
    ('MERCH_013', 'Riverside Gym & Fitness',    '7941', 'CO', FALSE, 49.00,  10.00),
    ('MERCH_014', 'Lucky Star Casino',          '7995', 'NV', TRUE,  300.00, 250.00),
    ('MERCH_015', 'Lakeside Pet Supply',        '5995', 'WA', FALSE, 42.00,  18.00)
ON CONFLICT (merchant_id) DO NOTHING;


-- =============================================================
-- VIEW: v_fraud_summary
-- Quick overview of flagged transactions with alert details
-- Useful for ad-hoc debugging and Athena cross-reference
-- =============================================================
CREATE OR REPLACE VIEW v_fraud_summary AS
SELECT
    t.transaction_id,
    t.timestamp,
    t.merchant_name,
    t.merchant_category_code,
    t.amount_usd,
    t.card_network,
    t.is_card_present,
    t.fraud_score,
    t.status,
    fa.rule_triggered,
    fa.alert_time,
    fa.sns_notification_sent
FROM transactions t
JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
ORDER BY fa.alert_time DESC;


-- =============================================================
-- VIEW: v_merchant_daily_summary
-- Aggregated daily totals per merchant
-- Mirrors what the Airflow batch job will produce in S3
-- =============================================================
CREATE OR REPLACE VIEW v_merchant_daily_summary AS
SELECT
    merchant_id,
    merchant_name,
    DATE(timestamp)                                     AS transaction_date,
    COUNT(*)                                            AS total_transactions,
    COUNT(*) FILTER (WHERE status = 'APPROVED')         AS approved_count,
    COUNT(*) FILTER (WHERE status = 'DECLINED')         AS declined_count,
    COUNT(*) FILTER (WHERE transaction_type = 'CHARGEBACK') AS chargeback_count,
    SUM(amount_usd) FILTER (WHERE status = 'APPROVED')  AS total_approved_usd,
    AVG(amount_usd) FILTER (WHERE status = 'APPROVED')  AS avg_ticket_usd,
    COUNT(*) FILTER (WHERE fraud_flag = TRUE)           AS fraud_flagged_count
FROM transactions
GROUP BY merchant_id, merchant_name, DATE(timestamp)
ORDER BY transaction_date DESC, total_approved_usd DESC;