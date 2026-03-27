# FinSecure — Real-Time Fraud Detection System

A real-time financial fraud detection system that streams transactions through Kafka, processes them with Apache Spark, runs XGBoost inference, and displays results on a live Streamlit dashboard.

## Architecture

```
CSV Data → Kafka Producer → Kafka Broker → Spark Structured Streaming → XGBoost Model → predictions.csv → Streamlit Dashboard
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Streaming | Apache Kafka |
| Stream Processing | Apache Spark (PySpark) |
| ML Model | XGBoost |
| Experiment Tracking | MLflow |
| API | FastAPI |
| Dashboard | Streamlit |
| Infra | Docker Compose |

## Project Structure

```
├── train_model.py        # XGBoost model training + MLflow tracking
├── kafka_producer.py     # Streams transactions to Kafka topic
├── spark_streaming.py    # Spark consumer + preprocessing + inference
├── api/main.py           # FastAPI prediction endpoint
├── dashboard/app.py      # Streamlit monitoring dashboard
├── docker-compose.yml    # Kafka + Zookeeper + Kafka UI
└── requirements.txt
```

## How to Run

**1. Start Kafka infrastructure**
```bash
docker-compose up -d
```

**2. Train the model**
```bash
python train_model.py
```

**3. Stream transactions to Kafka**
```bash
python kafka_producer.py
```

**4. Start Spark streaming job**
```bash
python spark_streaming.py
```

**5. Launch dashboard**
```bash
streamlit run dashboard/app.py
```

**6. (Optional) FastAPI prediction endpoint**
```bash
uvicorn api.main:app --reload --port 8000
```

## Features

- Real-time transaction scoring using XGBoost trained on 100K transactions
- Three-tier decision system: **approve / review / block** based on fraud probability
- Spark Structured Streaming consumes Kafka topic and runs inference on each micro-batch
- MLflow tracks model experiments, hyperparameters and metrics
- Dashboard shows fraud rate, blocked transactions, fraud by merchant category and device type
- Kafka UI available at `http://localhost:8080` for monitoring message flow
