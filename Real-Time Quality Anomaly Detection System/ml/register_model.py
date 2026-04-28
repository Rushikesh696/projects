import os
import sys
import logging
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import mlflow.tensorflow
from mlflow.tracking import MlflowClient
from dotenv import load_dotenv
from tensorflow import keras
import yaml

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "configs" / "config.yaml"
LOG_PATH = BASE_DIR / "logs" / "register_model.log"
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
        return yaml.safe_load(f)


def register_isolation_forest(config: dict, client: MlflowClient) -> str:
    model_name = config["mlflow"]["registered_model_name"] + "-isolation-forest"

    iso_forest = joblib.load(BASE_DIR / "models" / "isolation_forest.pkl")
    log.info("Isolation Forest loaded from disk.")

    with mlflow.start_run(run_name="register-isolation-forest"):
        mlflow.log_params({
            "model_type": "IsolationForest",
            "contamination": config["anomaly_detection"]["contamination"],
        })
        mlflow.sklearn.log_model(
            sk_model=iso_forest,
            artifact_path="isolation_forest",
            registered_model_name=model_name,
        )
        log.info(f"Isolation Forest registered as '{model_name}'")

    return model_name


def register_autoencoder(config: dict, client: MlflowClient) -> str:
    model_name = config["mlflow"]["registered_model_name"] + "-autoencoder"

    autoencoder = keras.models.load_model(BASE_DIR / "models" / "autoencoder.h5", compile=False)
    log.info("Autoencoder loaded from disk.")

    with mlflow.start_run(run_name="register-autoencoder"):
        mlflow.log_params({
            "model_type": "Autoencoder",
            "alert_threshold": config["anomaly_detection"]["alert_threshold"],
        })
        mlflow.tensorflow.log_model(
            model=autoencoder,
            artifact_path="autoencoder",
            registered_model_name=model_name,
        )
        log.info(f"Autoencoder registered as '{model_name}'")

    return model_name


def transition_to_staging(client: MlflowClient, model_name: str) -> None:
    versions = client.get_latest_versions(model_name, stages=["None"])
    if not versions:
        log.warning(f"No versions found for '{model_name}' — skipping transition.")
        return

    latest = versions[0]
    client.transition_model_version_stage(
        name=model_name,
        version=latest.version,
        stage="Staging",
    )
    log.info(f"'{model_name}' v{latest.version} → Staging")


def main():
    config = load_config()

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT_NAME"))
    client = MlflowClient()

    iso_model_name = register_isolation_forest(config, client)
    ae_model_name = register_autoencoder(config, client)

    transition_to_staging(client, iso_model_name)
    transition_to_staging(client, ae_model_name)

    log.info("Registration complete. Both models are in Staging.")


if __name__ == "__main__":
    main()
