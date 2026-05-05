# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**Real-Time Quality Anomaly Detection System** — Serum Institute of India Pvt. Ltd., Quality Assurance Division.

ML-based anomaly detection on synthetic real-time pharma QA event data (499,294 rows, 2020–2025). Full MLOps lifecycle: raw data ingestion via CDC → medallion layers → model training → serving → monitoring → automated retraining.

## Running the Stack

```bash
# Activate virtual environment first
source venv/bin/activate

# Start all services (Zookeeper, Kafka, PostgreSQL, Debezium, MLflow, Prometheus, Grafana, Kafka UI)
docker-compose up -d

# Stop all services
docker-compose down

# View logs for a specific service
docker-compose logs -f kafka
docker-compose logs -f postgres
```

**Service ports:**
| Service | Port |
|---------|------|
| PostgreSQL | 5433 (mapped from 5432) |
| Kafka (external) | 29092 |
| Debezium REST API | 8083 |
| MLflow UI | 5000 |
| FastAPI | 8000 |
| Prometheus | 9090 |
| Grafana | 3000 |
| Kafka UI | 8080 |
| Streamlit | 8501 |

## Pipeline Execution Order

```bash
# Step 1 — Register Debezium connector (after docker-compose up)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @kafka/debezium_config.json

# Step 2 — Start CDC consumer (keep running in background)
python kafka/consumer.py

# Step 3 — Load CSV into source DB (triggers CDC → Kafka → Bronze)
python kafka/producer.py

# Step 4 — Bronze → Silver → Gold transformations
python pipeline/bronze_to_silver.py
python pipeline/silver_to_gold.py

# Step 5 — Train and evaluate models (run notebooks, then:)
python ml/evaluate_model.py
python ml/register_model.py

# Step 6 — Start API
uvicorn api.main:app --host 0.0.0.0 --port 8000

# Step 7 — Start dashboard
streamlit run dashboard/app.py
```

## Running Tests

```bash
# All tests
pytest tests/

# Unit tests only
pytest tests/unit/

# Integration tests only (requires running stack)
pytest tests/integration/

# Single test file
pytest tests/unit/test_feature_engineering.py -v

# Single test function
pytest tests/unit/test_model_predict.py::test_function_name -v
```

## Architecture: CDC + Medallion

**Critical constraint: CSV is NEVER loaded directly into Bronze.** The flow is:

```
generate_data.py → data/bronze/serum_qa_realtime_raw.csv
                              ↓
                  source.qa_events (PostgreSQL)    ← only entry point
                              ↓  Debezium CDC → Kafka topic: serum.source.qa_events
                  bronze.qa_events                 ← append-only, never modified
                              ↓
                  silver.qa_events_clean            ← deduped, imputed, normalized
                              ↓
                  gold.qa_features                  ← rolling rates, ML-ready features
                              ↓
                  MLflow (Isolation Forest + Autoencoder)
                              ↓
                  FastAPI (JWT auth, /predict, /alerts)
                              ↓
                  Streamlit + Grafana + Evidently AI drift detection
                              ↓
                  Airflow DAGs (daily pipeline + monthly retrain)
```

All 5 schemas live in a single PostgreSQL instance (`serum_mlmodels` DB): `source`, `bronze`, `silver`, `gold`, `public` (alerts table).

## Configuration

All config flows through two files — never hardcode values:
- `.env` — secrets and environment-specific values (DB credentials, ports). **This file is not checked in — create it from the keys referenced in `configs/config.yaml` before running any service.**
- `configs/config.yaml` — app config that reads from env vars via `${VAR}` substitution

Key config values: `anomaly_detection.contamination = 0.01`, `rolling_window_days = 7`, `alert_threshold = 0.7`, `deviation_spike_multiplier = 2.5`.

## Dataset

`data/bronze/serum_qa_realtime_raw.csv` — 499,294 rows, 268MB. Target column: `is_anomaly` (0/1, 0.51% positive rate). Intentional noise: ~7% missing values in `product`/`system`/`root_cause`, ~2% duplicate rows, ~5% mislabeled `severity`. Silver layer must handle all of these.

## Build Status

### Infrastructure & Pipeline
- **Step 1 — Infrastructure**: `docker-compose.yml`, `configs/config.yaml`, `init_db.sql`, `Dockerfile.mlflow`, `monitoring/prometheus.yml` ✅
- **Step 2 — CDC Pipeline**: `kafka/producer.py`, `kafka/consumer.py` (with DLQ), Debezium connector. ~499,294 rows in `bronze.qa_events` ✅
- **Step 3 — Medallion Pipeline**: `pipeline/bronze_to_silver.py`, `pipeline/silver_to_gold.py`, `pipeline/validate_bronze.py`, `pipeline/validate_silver.py`. bronze=406,642 → silver=392,619 → gold=392,619 ✅
- **Step 4 — Notebooks**: `01_data_cleaning.ipynb` (EDA, anomaly rate 0.46%, COVID spike 2020–21), `02_feature_engineering.ipynb` (rolling features), `03_anomaly_detection.ipynb` (IF AUC=0.96, AE AUC=0.12, logged to MLflow) ✅

### ML & Serving
- **Step 5 — Evaluation + Registration**: `ml/evaluate_model.py`, `ml/register_model.py` — evaluates both models, registers to MLflow Model Registry as `serum_anomaly_detector-isolation-forest` and `serum_anomaly_detector-autoencoder`, transitions to Staging ✅
- **Step 6 — API**: `api/main.py`, `api/routes/auth.py`, `api/routes/predict.py`, `api/routes/alerts.py` — JWT auth, `/v1/predict`, `/v1/alerts`, `/health`, `/ready`. Demo credentials: `serum_qa` / `serum_pass123` ✅
- **Step 7 — Dashboard**: `dashboard/app.py` — KPI row, 7-day trend, system breakdown, severity breakdown, alert feed, system drill-down. Auto-refreshes every 30s ✅

### Orchestration & DevOps
- **Step 8 — Airflow DAGs**: `airflow/dags/daily_pipeline.py` (daily bronze→validate→silver→validate→gold), `airflow/dags/monthly_retrain.py` (Evidently AI drift check → conditional retrain → evaluate → register) ✅
- **Step 9 — CI/CD**: `ci.yml` (lint + unit tests + config validation) and `cd.yml` (integration tests + API smoke test + model artifact check). Both passing on GitHub Actions ✅
- **DVC**: `dvc.yaml` (4 pipeline stages) + `params.yaml` (tracked parameters). Run `git init && dvc init && dvc add data/bronze/serum_qa_realtime_raw.csv` to activate ⚠️ partial

### Tests
- `tests/unit/test_consumer.py` — `parse_message()`: valid, metadata skip, malformed, timestamp conversion ✅
- `tests/unit/test_feature_engineering.py` — `prepare_features()`: shape, NaN fill, binary y, missing column ✅
- `tests/unit/test_api_auth.py` — JWT create, verify valid, reject invalid/wrong-secret/missing-sub ✅
- `tests/unit/test_model_predict.py` — IF score negation, -1→1 mapping, AE reconstruction error, metrics keys ✅
- `tests/integration/test_db_connection.py` — PostgreSQL, all 5 schemas, all 5 tables, row count assertions ✅
- `tests/integration/test_api_endpoints.py` — /health, /ready, /token auth, /v1/predict, /v1/alerts ✅

## Key Implementation Notes

- **PySpark** is used throughout (producer, pipeline, notebooks) — not pandas. PostgreSQL JDBC jar at `drivers/postgresql-42.7.3.jar` is required for Spark ↔ PostgreSQL writes
- **File logging** is used on every long-running process — each gets its own log file in `logs/` directory. Pattern: `LOG_PATH = BASE_DIR / "logs" / "<service>.log"`, `LOG_PATH.parent.mkdir(exist_ok=True)`. Pipeline scripts use both `FileHandler` and `StreamHandler`; `consumer.py` uses `FileHandler` only (intentional — keeps daemon output out of stdout)
- **consumer.py** inserts row by row (not batched) — intentional design so no rows are lost on crash. Uses `kafka-python` library (not PySpark). Debezium timestamps arrive as microseconds since epoch — divide by `1_000_000` before `datetime.fromtimestamp()`. Failed messages (parse or insert) are published to DLQ topic `serum.source.qa_events.dlq` with `error_type`, `error`, `kafka_partition`, `kafka_offset`, `raw_message`
- **`requirements.txt`** — complete and up to date. Install with `pip install -r requirements.txt`
- **validate_bronze.py / validate_silver.py** — use `ge.from_pandas()` API (no full GE project directory needed). Sample 10K rows for speed. `sys.exit(1)` on critical failures so Airflow marks the task failed
- **PostgreSQL WAL** is configured for logical replication (`wal_level=logical`) in docker-compose — required for Debezium CDC to work
- **Debezium connector** uses `pgoutput` plugin, monitors `source.qa_events` table only, publishes to Kafka topic `serum.source.qa_events`
- **Kafka external port** is 29092 (host) vs 9092 (internal container-to-container)
- **MLflow** server is v3.11.1 (Dockerfile.mlflow), local client is v3.11.1. Uses PostgreSQL backend + Docker volume for artifacts. Artifact proxy enabled via `--serve-artifacts` + `--artifacts-destination /mlflow/artifacts` + `--default-artifact-root mlflow-artifacts:/` — required so notebook client uploads through REST API instead of writing to disk directly. Model registry stages: `Staging` → `Production`
- **Model files**: `models/isolation_forest.pkl` (joblib), `models/autoencoder.h5` (keras), `models/scaler.pkl` (joblib — StandardScaler, must be applied before inference)
- **Gold layer features** (primary ML inputs — all computed per `system` over rolling windows):
  - `deviation_count_7d`, `deviation_count_30d`
  - `complaint_rate_7d`, `capa_overdue_ratio_30d`, `oos_count_7d`
  - `critical_ratio_7d`, `major_ratio_7d`
  - `unusual_event_count_7d`, `material_complaint_count_7d`, `quality_risk_open_7d`
- **Python version**: 3.10 (venv at `venv/`)

## Industry-Level Improvements Status

### Tier 1 — Done
- **Great Expectations** — `pipeline/validate_bronze.py` + `pipeline/validate_silver.py`. Hooked into Airflow daily DAG. Reports → `logs/ge_bronze_report.json`, `logs/ge_silver_report.json` ✅
- **DVC** — `dvc.yaml` + `params.yaml` written. Run `git init && dvc init && dvc add data/bronze/serum_qa_realtime_raw.csv` to activate ✅ partial
- **Drift-triggered retraining** — `airflow/dags/monthly_retrain.py` uses Evidently AI `DataDriftPreset`, retrains if >20% features drift ✅
- **Dead Letter Queue** — `kafka/consumer.py` publishes `ParseError` + `InsertError` failures to `serum.source.qa_events.dlq` with partition, offset, raw message ✅

### Tier 2 — Not yet implemented
- **Schema Registry (Confluent)** — Avro schemas for Kafka CDC messages
- **Shadow mode deployment** — run Staging + Production models in parallel before promoting
- **SHAP values per prediction** — store in alerts table for pharma audit trail
- **Structured JSON logging** (`structlog`) — queryable structured log fields
- **CI/CD GitHub Actions** — `ci.yml` + `cd.yml` workflows ✅

### Tier 3 — Partially done
- API versioning (`/v1/predict`) ✅, `/health` + `/ready` ✅, pre-commit hooks (black, isort, flake8) — not set up

## Key Pharma QA Terms

- **OOS**: Out of Specification
- **CAPA**: Corrective Action and Preventive Action
- **ADE**: Adverse Drug Event
- **QPM**: Quality Performance Metrics
- **LAR**: Lot Acceptance Rate
- **PQCR**: Product Quality Complaint Rate
- **IOOSR**: Invalidated OOS Rate
