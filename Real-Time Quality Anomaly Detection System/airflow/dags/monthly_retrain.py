"""
Monthly Retrain DAG  (Tier 1 — drift-triggered retraining)
Runs on the 1st of each month:
  1. check_drift     — Evidently AI drift report on gold.qa_features
  2. branch          — retrain only if >20% features have drifted
  3. retrain_models  — re-runs anomaly detection notebooks via papermill
  4. evaluate_models — ml/evaluate_model.py
  5. register_models — ml/register_model.py

Install before use:
    pip install apache-airflow evidently papermill
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from airflow import DAG

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PYTHON = str(PROJECT_ROOT / "venv" / "bin" / "python")
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

DEFAULT_ARGS = {
    "owner": "serum_qa",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

DRIFT_THRESHOLD = 0.20  # retrain if >20% of features have drifted
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


def run_drift_check(**context):
    """
    Compares reference data (oldest 70% of gold records) vs
    current data (most recent 30%) using Evidently DataDriftPreset.
    Pushes drift_detected=True/False to XCom.
    """
    import os

    import pandas as pd
    import psycopg2
    from dotenv import load_dotenv
    from evidently.metric_preset import DataDriftPreset
    from evidently.report import Report

    load_dotenv(PROJECT_ROOT / ".env")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    df = pd.read_sql(
        f"SELECT {', '.join(FEATURE_COLS)}, timestamp FROM gold.qa_features ORDER BY timestamp",
        conn,
    )
    conn.close()

    split = int(len(df) * 0.70)
    reference = df.iloc[:split][FEATURE_COLS]
    current = df.iloc[split:][FEATURE_COLS]

    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=reference, current_data=current)

    result = report.as_dict()
    drift_share = result["metrics"][0]["result"]["share_of_drifted_columns"]
    drift_detected = drift_share > DRIFT_THRESHOLD

    print(f"Drifted feature share: {drift_share:.2%} (threshold: {DRIFT_THRESHOLD:.0%})")
    print(f"Drift detected: {drift_detected}")

    report_path = PROJECT_ROOT / "logs" / "drift_report.html"
    report.save_html(str(report_path))
    print(f"Drift report saved to {report_path}")

    context["ti"].xcom_push(key="drift_detected", value=drift_detected)
    context["ti"].xcom_push(key="drift_share", value=drift_share)


def branch_on_drift(**context):
    drift_detected = context["ti"].xcom_pull(task_ids="check_drift", key="drift_detected")
    if drift_detected:
        print("Drift detected — proceeding with retraining.")
        return "retrain_models"
    print("No significant drift — skipping retraining.")
    return "skip_retrain"


with DAG(
    dag_id="serum_monthly_retrain",
    description="Monthly drift-triggered model retraining",
    schedule_interval="0 2 1 * *",  # 02:00 on the 1st of each month
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["serum", "mlops", "retrain", "drift"],
) as dag:

    check_drift = PythonOperator(
        task_id="check_drift",
        python_callable=run_drift_check,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_drift",
        python_callable=branch_on_drift,
    )

    skip_retrain = EmptyOperator(task_id="skip_retrain")

    retrain_models = BashOperator(
        task_id="retrain_models",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON} -m papermill "
            f"{NOTEBOOKS_DIR}/03_anomaly_detection.ipynb "
            f"{NOTEBOOKS_DIR}/03_anomaly_detection_retrain.ipynb "
            f"-p run_type retrain"
        ),
    )

    evaluate_models = BashOperator(
        task_id="evaluate_models",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} ml/evaluate_model.py",
    )

    register_models = BashOperator(
        task_id="register_models",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} ml/register_model.py",
    )

    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed_min_one_success",
    )

    check_drift >> branch >> [retrain_models, skip_retrain]
    retrain_models >> evaluate_models >> register_models >> done
    skip_retrain >> done
