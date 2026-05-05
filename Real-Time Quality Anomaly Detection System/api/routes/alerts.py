import os
import logging
from datetime import datetime
from typing import List, Optional

import psycopg2
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from api.routes.auth import verify_token

load_dotenv()

router = APIRouter()
log = logging.getLogger(__name__)


class AlertRecord(BaseModel):
    id: int
    timestamp: datetime
    event_id: Optional[str] = None
    event_type: Optional[str] = None
    system: Optional[str] = None
    product: Optional[str] = None
    severity: Optional[str] = None
    anomaly_score: Optional[float] = None
    alert_type: Optional[str] = None
    alert_reason: Optional[str] = None
    status: Optional[str] = None


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


@router.get("/alerts", response_model=List[AlertRecord])
def get_alerts(
    limit: int = Query(50, ge=1, le=500),
    status: Optional[str] = Query(None),
    _: str = Depends(verify_token),
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            if status:
                cur.execute(
                    "SELECT id, timestamp, event_id, event_type, system, product, severity, "
                    "anomaly_score, alert_type, alert_reason, status "
                    "FROM public.alerts WHERE status = %s ORDER BY timestamp DESC LIMIT %s",
                    (status, limit),
                )
            else:
                cur.execute(
                    "SELECT id, timestamp, event_id, event_type, system, product, severity, "
                    "anomaly_score, alert_type, alert_reason, status "
                    "FROM public.alerts ORDER BY timestamp DESC LIMIT %s",
                    (limit,),
                )
            rows = cur.fetchall()
            cols = [
                "id",
                "timestamp",
                "event_id",
                "event_type",
                "system",
                "product",
                "severity",
                "anomaly_score",
                "alert_type",
                "alert_reason",
                "status",
            ]
            return [AlertRecord(**dict(zip(cols, row))) for row in rows]
    finally:
        conn.close()
