import joblib
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import os

app = FastAPI(title="PredictMaint API", description="Real-time Machine Failure Prediction", version="1.0.0")

MODEL_PATH = "data/models/failure_predictor.pkl"
model = None


@app.on_event("startup")
def load_model():
    global model
    if os.path.exists(MODEL_PATH):
        model = joblib.load(MODEL_PATH)
        print("Model loaded successfully.")
    else:
        print(f"Warning: Model not found at {MODEL_PATH}. Train the model first.")


class SensorReading(BaseModel):
    machine_id: str
    machine_type: str
    location: str
    criticality: str
    manufacturer: str
    age_years: float
    temperature: float
    vibration: float
    pressure: float
    rpm: float
    voltage: float
    temperature_lag1: Optional[float] = None
    vibration_lag1: Optional[float] = None
    pressure_lag1: Optional[float] = None
    rpm_lag1: Optional[float] = None
    temperature_mean_5m: Optional[float] = None
    vibration_mean_5m: Optional[float] = None
    pressure_mean_5m: Optional[float] = None
    is_anomaly: Optional[bool] = False
    anomaly_count_5m: Optional[int] = 0
    hour: Optional[int] = 12
    day_of_week: Optional[int] = 1
    is_night_shift: Optional[int] = 0


class PredictionResponse(BaseModel):
    machine_id: str
    failure_probability: float
    risk_level: str
    recommendation: str


def get_risk_level(prob: float) -> tuple:
    if prob >= 0.75:
        return "CRITICAL", "Immediate shutdown and inspection required!"
    elif prob >= 0.50:
        return "HIGH", "Schedule maintenance within 2 hours."
    elif prob >= 0.25:
        return "MEDIUM", "Monitor closely and plan maintenance soon."
    else:
        return "LOW", "Machine operating normally."


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None}


@app.post("/predict", response_model=PredictionResponse)
def predict(reading: SensorReading):
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded. Train the model first.")

    from sklearn.preprocessing import LabelEncoder
    data = reading.dict()
    machine_id = data.pop("machine_id")

    cat_cols = ["machine_type", "location", "criticality", "manufacturer"]
    for c in cat_cols:
        le = LabelEncoder()
        data[c] = le.fit_transform([data[c]])[0]

    df = pd.DataFrame([data])
    df = df.fillna(0)

    prob = float(model.predict_proba(df)[0][1])
    risk_level, recommendation = get_risk_level(prob)

    return PredictionResponse(
        machine_id=machine_id,
        failure_probability=round(prob, 4),
        risk_level=risk_level,
        recommendation=recommendation,
    )


@app.get("/machines/{machine_id}/status")
def machine_status(machine_id: str):
    return {
        "machine_id": machine_id,
        "message": "Submit a POST /predict with sensor readings to get failure probability."
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
