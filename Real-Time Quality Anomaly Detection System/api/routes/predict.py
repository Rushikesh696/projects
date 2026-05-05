import logging
import os

import numpy as np
import psycopg2
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from api.routes.auth import verify_token

load_dotenv()

router = APIRouter()
log = logging.getLogger(__name__)

FEATURE_COLS = [
    "deviation_count_7d",
    "deviation_count_30d",
    "complaint_rate_7d",
    "capa_overdue_ratio_30d",
    "oos_count_7d",
    "critical_ratio_7d",
    "major_ratio_7d",
    "unusual_event_count_7d",
    "material_complaint_count_7d",
    "quality_risk_open_7d",
]


class PredictRequest(BaseModel):
    event_id: str
    event_type: str
    system: str
    product: str
    severity: str
    deviation_count_7d: float = 0.0
    deviation_count_30d: float = 0.0
    complaint_rate_7d: float = 0.0
    capa_overdue_ratio_30d: float = 0.0
    oos_count_7d: float = 0.0
    critical_ratio_7d: float = 0.0
    major_ratio_7d: float = 0.0
    unusual_event_count_7d: float = 0.0
    material_complaint_count_7d: float = 0.0
    quality_risk_open_7d: float = 0.0


class PredictResponse(BaseModel):
    event_id: str
    is_anomaly: int
    anomaly_score: float
    alert_triggered: bool
    model: str = "isolation_forest"


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def save_alert(conn, req: PredictRequest, anomaly_score: float):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.alerts
                (event_id, event_type, system, product, severity,
                 anomaly_score, alert_type, alert_reason, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                req.event_id,
                req.event_type,
                req.system,
                req.product,
                req.severity,
                anomaly_score,
                "ml_based",
                f"Isolation Forest anomaly score: {anomaly_score:.4f}",
                "open",
            ),
        )
        conn.commit()


@router.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest, request: Request, _: str = Depends(verify_token)):
    scaler = request.app.state.scaler
    iso_forest = request.app.state.iso_forest
    config = request.app.state.config
    threshold = config["anomaly_detection"]["alert_threshold"]

    features = np.array([[getattr(req, col) for col in FEATURE_COLS]])
    X_scaled = scaler.transform(features)

    raw_score = iso_forest.decision_function(X_scaled)[0]
    anomaly_score = float(-raw_score)
    pred = iso_forest.predict(X_scaled)[0]
    is_anomaly = 1 if pred == -1 else 0

    alert_triggered = bool(is_anomaly and anomaly_score >= threshold)

    if alert_triggered:
        try:
            conn = get_db_conn()
            save_alert(conn, req, anomaly_score)
            conn.close()
            log.info(f"Alert saved — event_id={req.event_id}, score={anomaly_score:.4f}")
        except Exception as e:
            log.error(f"Failed to save alert: {e}")

    return PredictResponse(
        event_id=req.event_id,
        is_anomaly=is_anomaly,
        anomaly_score=round(anomaly_score, 4),
        alert_triggered=alert_triggered,
    )
