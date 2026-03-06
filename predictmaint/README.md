# PredictMaint - Real-Time Predictive Maintenance System

## Problem Statement

Manufacturing plants operate hundreds of machines (pumps, compressors, motors, turbines, conveyors) that are critical to production continuity. **Unexpected equipment failures** cause unplanned downtime, which is one of the most expensive problems in industrial operations — costing manufacturers an estimated **$50 billion annually** worldwide.

Traditional maintenance strategies are either:
- **Reactive**: Fix after failure — leads to costly emergency repairs, production halts, and safety hazards
- **Preventive**: Fixed-schedule servicing — leads to unnecessary maintenance on healthy machines and missed failures on degraded ones

Neither approach uses the rich sensor data (temperature, vibration, pressure, RPM, voltage) that modern IoT-enabled machines continuously generate.

**PredictMaint** solves this by building a real-time IoT streaming pipeline that:
1. Ingests live sensor readings from 50 machines via Apache Kafka
2. Detects anomalies and enriches data with machine metadata
3. Engineers time-series features (rolling stats, lag values, rate of change)
4. Trains an XGBoost classifier to predict **equipment failure 4 hours in advance**
5. Serves predictions via a FastAPI endpoint and visualizes machine health on a Streamlit dashboard

This enables maintenance teams to **act before failure occurs**, reducing downtime by up to 40% and maintenance costs by up to 25%.

---

## Architecture

```
IoT Sensors (50 Machines)
         ↓
Kafka Topic: iot.sensor.readings
         ↓
[Bronze Layer] → Raw sensor ingestion → Parquet (append-only, partitioned by date)
         ↓
[Silver Layer] → Validate, deduplicate, flag anomalies, join machine master
         ↓
[Gold Layer]   → Rolling stats (5/15/30 min), lag features, rate of change
         ↓
[ML Model]     → XGBoost binary classifier → P(failure in next 4 hrs)
         ↓
[FastAPI]      → /predict endpoint → risk level + recommendation
         ↓
[Streamlit]    → Live dashboard → machine health, risk scores, sensor trends
```

---

## Project Structure

```
predictmaint/
├── docker/
│   └── docker-compose.yml           # Kafka + Zookeeper + Kafka UI
├── data_generation/
│   ├── generate_master_data.py      # 50 machines + 200 failure history records
│   └── kafka_producer.py            # Simulate live sensor readings with degradation
├── spark/
│   ├── bronze/
│   │   └── ingest_stream.py         # Raw Kafka stream → Parquet
│   ├── silver/
│   │   └── clean_transform.py       # Clean, validate, enrich, anomaly flags
│   └── gold/
│       └── feature_engineering.py   # Rolling stats, lags, rate of change
├── ml/
│   └── train_model.py               # XGBoost training with MLflow tracking
├── api/
│   └── main.py                      # FastAPI prediction service
├── dashboard/
│   └── app.py                       # Streamlit live monitoring dashboard
├── data/
│   ├── raw/                         # machine_master.csv, failure_history.csv
│   ├── bronze/                      # Raw sensor parquet
│   ├── silver/                      # Enriched sensor parquet
│   ├── gold/                        # ML feature table
│   └── models/                      # Saved ML models
└── requirements.txt
```

---

## Key Features

- **Real-time IoT streaming** via Apache Kafka (`iot.sensor.readings` topic)
- **5 sensors per machine**: temperature, vibration, pressure, RPM, voltage
- **Degradation simulation**: gradual sensor drift leading to failure events
- **Anomaly detection**: per-sensor threshold breach flags
- **Rich feature engineering**:
  - Rolling mean, std, min, max over 5 / 15 / 30 minute windows
  - Lag features: lag-1, lag-5, lag-10 readings per sensor
  - Rate of change per sensor
  - Anomaly count in 5-minute window
- **XGBoost classifier** with MLflow experiment tracking
- **FastAPI** with risk levels: LOW / MEDIUM / HIGH / CRITICAL
- **Streamlit dashboard** with live sensor trends and fleet overview

---

## Sensors & Thresholds

| Sensor | Unit | Normal Range | Critical Threshold |
|---|---|---|---|
| Temperature | °C | 40 - 120 | 105 - 150 (by type) |
| Vibration | mm/s | 0.1 - 4.0 | 4.0 - 8.0 (by type) |
| Pressure | bar | 0.5 - 30 | 7.0 - 40 (by type) |
| RPM | rev/min | 100 - 5000 | 500 - 6000 (by type) |
| Voltage | V | 200 - 240 | 260 |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka, PySpark Structured Streaming |
| Storage | Apache Parquet (time-partitioned) |
| ML | XGBoost, scikit-learn |
| MLOps | MLflow |
| API | FastAPI, Uvicorn |
| Dashboard | Streamlit, Plotly |
| Data | Pandas, NumPy, PyArrow |

---

## Getting Started

### 1. Start Kafka Infrastructure
```bash
cd docker
docker-compose up -d
```

### 2. Generate Master Data
```bash
python data_generation/generate_master_data.py
```

### 3. Start IoT Kafka Producer
```bash
python data_generation/kafka_producer.py --speed normal
```

### 4. Run Spark Pipeline
```bash
# Bronze
spark-submit spark/bronze/ingest_stream.py

# Silver
spark-submit spark/silver/clean_transform.py

# Gold
spark-submit spark/gold/feature_engineering.py
```

### 5. Train ML Model
```bash
python ml/train_model.py
```

### 6. Start API
```bash
cd api
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 7. Launch Dashboard
```bash
streamlit run dashboard/app.py
```

### 8. Install Dependencies
```bash
pip install -r requirements.txt
```

---

## API Usage

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "machine_id": "M001",
    "machine_type": "Pump",
    "location": "Plant-A",
    "criticality": "High",
    "manufacturer": "Siemens",
    "age_years": 5.2,
    "temperature": 98.5,
    "vibration": 5.8,
    "pressure": 11.2,
    "rpm": 2100,
    "voltage": 238
  }'
```

**Response:**
```json
{
  "machine_id": "M001",
  "failure_probability": 0.87,
  "risk_level": "CRITICAL",
  "recommendation": "Immediate shutdown and inspection required!"
}
```
