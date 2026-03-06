# PharmaCast - Pharmaceutical Demand Forecasting System

## Problem Statement

The pharmaceutical supply chain faces a critical challenge: **unpredictable drug demand** leads to either stockouts (life-threatening) or overstocking (costly wastage). Traditional forecasting methods fail to account for real-time factors such as seasonal disease outbreaks, regional festivals, cold chain constraints, and channel-specific demand variations.

Hospitals, clinics, and retail pharmacies often run out of essential medicines during peak demand periods (flu season, monsoon, festivals), while simultaneously holding excess inventory of slow-moving drugs. This results in:
- **Patient risk** due to unavailability of critical medicines
- **Revenue loss** from expired or overstocked inventory
- **Inefficient supply chain** with poor replenishment planning

**PharmaCast** solves this by building a real-time streaming data pipeline that ingests live sales transactions, enriches them with contextual data (seasonal events, regional demographics), and engineers ML-ready features to forecast drug demand accurately — enabling proactive inventory management.

---

## Architecture

```
Kafka Producer (Simulated ERP/POS)
         ↓
Kafka Topic: pharma.sales.transactions
         ↓
[Bronze Layer] → Raw ingestion → Parquet (append-only)
         ↓
[Silver Layer] → Clean, deduplicate, enrich → sales_enriched
         ↓
[Gold Layer]   → Aggregate, feature engineer → ml_features (28 features)
         ↓
[ML Models]    → XGBoost / LightGBM / Prophet → Demand Forecast
         ↓
[FastAPI]      → Serve predictions via REST API
         ↓
[Streamlit]    → Visualize forecasts & inventory insights
```

---

## Project Structure

```
pharmacast/
├── docker/
│   └── docker-compose.yml         # Kafka + Zookeeper + Kafka UI
├── data_generation/
│   ├── generate_master_data.py    # Generate drug, region, seasonal event master data
│   └── kafka_producer.py          # Simulate real-time pharma sales transactions
├── spark/
│   ├── bronze/
│   │   └── ingest_stream.py       # Raw Kafka stream ingestion
│   ├── silver/
│   │   └── clean_transform.py     # Cleaning, dedup, enrichment
│   └── gold/
│       └── feature_engineering.py # ML feature engineering (28 features)
├── data/
│   ├── raw/                       # Master CSVs (drug, region, events)
│   ├── bronze/                    # Raw parquet transactions
│   ├── silver/                    # Enriched parquet data
│   └── gold/                      # ML-ready feature table
├── ml/                            # Model training (coming soon)
├── api/                           # FastAPI prediction service (coming soon)
├── dashboard/                     # Streamlit dashboard (coming soon)
└── requirements.txt
```

---

## Key Features

- **Real-time ingestion** via Apache Kafka with 3-partition topic
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **200 drugs** across 10 categories, **15 regions** across India
- **Seasonal event modeling**: festivals, disease seasons, holidays
- **28 ML features**: lag (1/3/6/12m), rolling averages, channel breakdown, seasonality flags
- **Data quality flags**: deduplication, null handling, return detection (`high_return_flag`)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka, PySpark Structured Streaming |
| Storage | Apache Parquet (partitioned) |
| ML | XGBoost, LightGBM, Prophet, scikit-learn |
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

### 3. Start Kafka Producer
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

### 5. Install Dependencies
```bash
pip install -r requirements.txt
```
