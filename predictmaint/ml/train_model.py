import os
import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
import joblib
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, roc_auc_score,
    confusion_matrix, f1_score, precision_score, recall_score
)
from sklearn.preprocessing import LabelEncoder

GOLD_PATH = "data/gold/ml_features"
MODEL_PATH = "data/models/failure_predictor.pkl"
MLFLOW_EXPERIMENT = "PredictMaint-FailurePredictor"

TARGET = "will_fail_in_4hrs"

DROP_COLS = [
    "reading_id", "machine_name", "timestamp", "reading_date",
    "ingestion_source", "ingestion_timestamp", "bronze_ingested_at",
    "silver_processed_at", "gold_processed_at", "ingestion_date",
    "degradation_factor"
]

CAT_COLS = ["machine_id", "machine_type", "location", "criticality", "manufacturer", "season"]


def load_data():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("TrainModel").getOrCreate()
    df = spark.read.parquet(GOLD_PATH).toPandas()
    spark.stop()
    return df


def preprocess(df):
    df = df.drop(columns=[c for c in DROP_COLS if c in df.columns], errors="ignore")
    df = df.dropna(subset=[TARGET])

    for c in CAT_COLS:
        if c in df.columns:
            le = LabelEncoder()
            df[c] = le.fit_transform(df[c].astype(str))

    df = df.fillna(df.median(numeric_only=True))
    return df


def train():
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    print("Loading data...")
    df = load_data()
    df = preprocess(df)

    X = df.drop(columns=[TARGET])
    y = df[TARGET].astype(int)

    print(f"Dataset: {X.shape[0]} rows, {X.shape[1]} features")
    print(f"Failure rate: {y.mean():.2%}")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    with mlflow.start_run(run_name="XGBoost-FailurePredictor"):
        params = {
            "n_estimators": 200,
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "scale_pos_weight": (y == 0).sum() / (y == 1).sum(),
            "random_state": 42,
            "eval_metric": "auc",
        }
        mlflow.log_params(params)

        model = XGBClassifier(**params)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        metrics = {
            "roc_auc":   round(roc_auc_score(y_test, y_prob), 4),
            "f1_score":  round(f1_score(y_test, y_pred), 4),
            "precision": round(precision_score(y_test, y_pred), 4),
            "recall":    round(recall_score(y_test, y_pred), 4),
        }
        mlflow.log_metrics(metrics)
        print("\nModel Metrics:")
        for k, v in metrics.items():
            print(f"  {k}: {v}")

        print("\nClassification Report:")
        print(classification_report(y_test, y_pred))

        # Save model
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        joblib.dump(model, MODEL_PATH)
        mlflow.xgboost.log_model(model, "model")
        print(f"\nModel saved to {MODEL_PATH}")

        # Feature importance
        importance = pd.Series(model.feature_importances_, index=X.columns)
        top10 = importance.nlargest(10)
        print("\nTop 10 Features:")
        print(top10)


if __name__ == "__main__":
    train()
