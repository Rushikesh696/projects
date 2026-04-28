\connect serum_mlmodels

-- ─────────────────────────────────────────
-- Serum Institute QA Anomaly Detection
-- PostgreSQL Schema Initialization
-- ─────────────────────────────────────────

-- Create all schemas 
CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ─────────────────────────────────────────
-- SOURCE SCHEMA — Simulates LIMS/MES/SAP
-- CSV is loaded here only
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS source.qa_events (
    id                        SERIAL PRIMARY KEY,
    timestamp                 TIMESTAMP,
    event_type                VARCHAR(50),
    event_id                  VARCHAR(50),
    product                   VARCHAR(100),
    system                    VARCHAR(100),
    root_cause                VARCHAR(100),
    severity                  VARCHAR(20),
    batch_id                  VARCHAR(50),
    status                    VARCHAR(50),
    resolution_days           FLOAT,
    closed_timestamp          TIMESTAMP,
    is_anomaly                INTEGER,
    is_covid_period           INTEGER,
    hour                      INTEGER,
    day_of_week               INTEGER,
    month                     INTEGER,
    year                      INTEGER,
    is_weekend                INTEGER,
    created_at                TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- BRONZE SCHEMA — Raw CDC events
-- Append-only, never modified
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.qa_events (
    id                        SERIAL PRIMARY KEY,
    source_id                 INTEGER,
    timestamp                 TIMESTAMP,
    event_type                VARCHAR(50),
    event_id                  VARCHAR(50),
    product                   VARCHAR(100),
    system                    VARCHAR(100),
    root_cause                VARCHAR(100),
    severity                  VARCHAR(20),
    batch_id                  VARCHAR(50),
    status                    VARCHAR(50),
    resolution_days           FLOAT,
    closed_timestamp          TIMESTAMP,
    is_anomaly                INTEGER,
    is_covid_period           INTEGER,
    hour                      INTEGER,
    day_of_week               INTEGER,
    month                     INTEGER,
    year                      INTEGER,
    is_weekend                INTEGER,
    cdc_operation             VARCHAR(10),   -- INSERT / UPDATE / DELETE
    cdc_ingested_at           TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- SILVER SCHEMA — Cleaned data
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.qa_events_clean (
    id                        SERIAL PRIMARY KEY,
    bronze_id                 INTEGER,
    timestamp                 TIMESTAMP,
    event_type                VARCHAR(50),
    event_id                  VARCHAR(50),
    product                   VARCHAR(100),
    system                    VARCHAR(100),
    root_cause                VARCHAR(100),
    severity                  VARCHAR(20),
    batch_id                  VARCHAR(50),
    status                    VARCHAR(50),
    resolution_days           FLOAT,
    closed_timestamp          TIMESTAMP,
    is_anomaly                INTEGER,
    is_covid_period           INTEGER,
    hour                      INTEGER,
    day_of_week               INTEGER,
    month                     INTEGER,
    year                      INTEGER,
    is_weekend                INTEGER,
    cleaned_at                TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- GOLD SCHEMA — Feature engineered data
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.qa_features (
    id                              SERIAL PRIMARY KEY,
    silver_id                       INTEGER,
    timestamp                       TIMESTAMP,
    event_type                      VARCHAR(50),
    product                         VARCHAR(100),
    system                          VARCHAR(100),
    severity                        VARCHAR(20),
    is_anomaly                      INTEGER,
    is_covid_period                 INTEGER,
    -- Rolling features
    deviation_count_7d              FLOAT,
    deviation_count_30d             FLOAT,
    complaint_rate_7d               FLOAT,
    capa_overdue_ratio_30d          FLOAT,
    oos_count_7d                    FLOAT,
    unusual_event_count_7d          FLOAT,
    material_complaint_count_7d     FLOAT,
    quality_risk_open_7d            FLOAT,
    -- Severity scores
    critical_ratio_7d               FLOAT,
    major_ratio_7d                  FLOAT,
    -- Time features
    hour                            INTEGER,
    day_of_week                     INTEGER,
    month                           INTEGER,
    is_weekend                      INTEGER,
    engineered_at                   TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- PUBLIC SCHEMA — Alerts table
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.alerts (
    id                        SERIAL PRIMARY KEY,
    timestamp                 TIMESTAMP DEFAULT NOW(),
    event_id                  VARCHAR(50),
    event_type                VARCHAR(50),
    system                    VARCHAR(100),
    product                   VARCHAR(100),
    severity                  VARCHAR(20),
    anomaly_score             FLOAT,
    alert_type                VARCHAR(50),   -- rule_based / ml_based
    alert_reason              TEXT,
    status                    VARCHAR(20) DEFAULT 'open'
);

