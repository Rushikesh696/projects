import os
import sys
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import joblib
import yaml
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from tensorflow import keras

from api.routes import auth, predict, alerts

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "configs" / "config.yaml"
LOG_PATH = BASE_DIR / "logs" / "api.log"
LOG_PATH.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH),
    ],
)
log = logging.getLogger(__name__)


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Loading model artifacts...")
    app.state.scaler = joblib.load(BASE_DIR / "models" / "scaler.pkl")
    app.state.iso_forest = joblib.load(BASE_DIR / "models" / "isolation_forest.pkl")
    app.state.autoencoder = keras.models.load_model(
        BASE_DIR / "models" / "autoencoder.h5", compile=False
    )
    app.state.config = load_config()
    log.info("Models loaded. API ready.")
    yield
    log.info("API shutting down.")


app = FastAPI(
    title="Serum QA Anomaly Detection API",
    description="Real-time pharma QA anomaly detection — Serum Institute of India",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, tags=["auth"])
app.include_router(predict.router, prefix="/v1", tags=["predict"])
app.include_router(alerts.router, prefix="/v1", tags=["alerts"])


@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok"}


@app.get("/ready", tags=["ops"])
def ready():
    return {"status": "ready", "models": ["isolation_forest", "autoencoder"]}
