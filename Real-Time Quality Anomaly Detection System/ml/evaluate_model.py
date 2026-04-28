import os
import sys
import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
from dotenv import load_dotenv
from sklearn.metrics import (
    precision_score, f1_score, recall_score, 
    roc_auc_score, confusion_matrix, roc_curve, ConfusionMatrixDisplay
)
from pyspark.sql import SparkSession
import yaml
from tensorflow import keras

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "configs" / "config.yaml"
LOG_PATH = BASE_DIR / "logs" / "evaluate_model.log"
LOG_PATH.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH),
    ]
)
log = logging.getLogger(__name__)

def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        content = os.path.expandvars(f.read())
    return yaml.safe_load(content) 
    
def load_artifacts():
    log.info("Loading model artifacts from MLflow...")
    scaler = joblib.load(BASE_DIR / "models" / "scaler.pkl")
    iso_forest = joblib.load(BASE_DIR / "models" / "isolation_forest.pkl")
    autoencoder = keras.models.load_model(BASE_DIR / "models" / "autoencoder.h5", compile=False)
    log.info("Artifacts loaded successfully.")
    return scaler, iso_forest, autoencoder


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("SerumQA-EvaluateModel")
        .config("spark.jars", str(BASE_DIR / "drivers" / "postgresql-42.7.3.jar"))
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def read_gold(spark: SparkSession) -> pd.DataFrame:
    jdbc_url = (
        f"jdbc:postgresql://"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5433')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

    properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }

    df = spark.read.jdbc(jdbc_url, "gold.qa_features", properties=properties)
    log.info(f"Read {df.count():,} rows from gold.qa_features")
    return df.toPandas()

FEATURE_COLS = [
    "deviation_count_7d", "deviation_count_30d",
    "complaint_rate_7d", "capa_overdue_ratio_30d",
    "oos_count_7d", "critical_ratio_7d", "major_ratio_7d",
    "unusual_event_count_7d", "material_complaint_count_7d", "quality_risk_open_7d"
    ]

def prepare_features(df: pd.DataFrame, scaler) -> tuple:
    X = df[FEATURE_COLS].fillna(0).values
    y = df["is_anomaly"].values
    X_scaled = scaler.transform(X)
    log.info(f"Features prepared. Shape: {X_scaled.shape}, Anomalies: {y.sum():,} ({y.mean() * 100:.2f}%)")
    return X_scaled, y


def evaluate_isolation_forest(iso_forest, X_scaled, y):
    log.info("Evaluating Isolation Forest...")
    raw_scores = iso_forest.decision_function(X_scaled)
    anomaly_scores = -raw_scores

    preds = iso_forest.predict(X_scaled)
    preds_binary = np.where(preds == -1, 1, 0)

    metrics = {
        "iso_precision": precision_score(y, preds_binary, zero_division=0),
        "iso_recall": recall_score(y, preds_binary, zero_division=0),
        "iso_f1": f1_score(y, preds_binary, zero_division=0),
        "iso_roc_auc": roc_auc_score(y, anomaly_scores),
    }

    log.info(f"IF - Precision: {metrics['iso_precision']:.4f}, Recall: {metrics['iso_recall']:.4f}, F1: {metrics['iso_f1']:.4f}, ROC-AUC: {metrics['iso_roc_auc']:.4f}")

    return metrics, preds_binary, anomaly_scores


def evaluate_autoencoder(autoencoder, X_scaled, y, threshold=0.7):
    log.info("Evaluating Autoencoder...")
    
    X_reconstructed = autoencoder.predict(X_scaled)
    recon_errors = np.mean(np.power(X_scaled - X_reconstructed, 2), axis=1)

    preds_binary = (recon_errors >= threshold).astype(int)

    metrics = {
        "ae_precision": precision_score(y, preds_binary, zero_division=0),
        "ae_recall": recall_score(y, preds_binary, zero_division=0),
        "ae_f1": f1_score(y, preds_binary, zero_division=0),
        "ae_roc_auc": roc_auc_score(y, recon_errors),
    }

    log.info(f"AE - Precision: {metrics['ae_precision']:.4f}, Recall: {metrics['ae_recall']:.4f}, F1: {metrics['ae_f1']:.4f}, ROC-AUC: {metrics['ae_roc_auc']:.4f}")

    return metrics, preds_binary, recon_errors

def log_to_mlflow(config, iso_metrics, ae_metrics, y,
                  iso_preds, iso_scores, ae_preds, ae_scores):
    
    mlflow.set_tracking_uri(config['mlflow']['tracking_uri'])
    mlflow.set_experiment(config['mlflow']['experiment_name'])

    with mlflow.start_run(run_name="Evaluation"):
        mlflow.log_params({
            "alert_threshold": config["anomaly_detection"]["alert_threshold"],
            "contamination": config["anomaly_detection"]["contamination"],
        })

        mlflow.log_metrics({**iso_metrics, **ae_metrics})
        for model_name, preds in [("isolation_forest", iso_preds), ("autoencoder", ae_preds)]:
            cm = confusion_matrix(y, preds)
            cm_display = ConfusionMatrixDisplay(cm, display_labels=["Normal", "Anomaly"])
            fig, ax = plt.subplots()
            cm_display.plot(ax=ax)
            ax.set_title(f"{model_name} Confusion Matrix")
            plot_path = BASE_DIR / "logs" / f"{model_name}_confusion_matrix.png"
            fig.savefig(plot_path)
            mlflow.log_artifact(str(plot_path))
            plt.close(fig)

        log.info("Metrics and plots logged to MLflow .")

def main():
    config = load_config()
    threshold = config["anomaly_detection"]["alert_threshold"]

    scaler, iso_forest, autoencoder = load_artifacts()

    spark = get_spark()
    try:
        df = read_gold(spark)
    finally:
        spark.stop()

    X_scaled, y = prepare_features(df, scaler)

    iso_metrics, iso_preds, iso_scores = evaluate_isolation_forest(iso_forest, X_scaled, y)
    ae_metrics, ae_preds, ae_scores = evaluate_autoencoder(autoencoder, X_scaled, y, threshold)

    log_to_mlflow(config, iso_metrics, ae_metrics, y, iso_preds, iso_scores, ae_preds, ae_scores)
    log.info("Model evaluation complete.")

if __name__ == "__main__":
    main()

    