"""
Daily Pipeline DAG
Runs Bronze → Silver → Gold transformations every day at midnight.

Install before use:
    pip install apache-airflow
    airflow db init
    airflow users create --username admin --password admin \
        --firstname Admin --lastname Admin --role Admin --email admin@serum.com
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PYTHON = str(PROJECT_ROOT / "venv" / "bin" / "python")

DEFAULT_ARGS = {
    "owner": "serum_qa",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="serum_daily_pipeline",
    description="Daily Bronze → Silver → Gold medallion pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["serum", "pipeline", "medallion"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} pipeline/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} pipeline/silver_to_gold.py",
    )

    def check_row_counts(**context):
        import os

        import psycopg2
        from dotenv import load_dotenv

        load_dotenv(PROJECT_ROOT / ".env")

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5433)),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.qa_events_clean")
            silver_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gold.qa_features")
            gold_count = cur.fetchone()[0]
        conn.close()

        print(f"Silver rows: {silver_count:,}")
        print(f"Gold rows:   {gold_count:,}")

        if silver_count == 0:
            raise ValueError("Silver table is empty after pipeline run.")
        if gold_count == 0:
            raise ValueError("Gold table is empty after pipeline run.")

        context["ti"].xcom_push(key="silver_count", value=silver_count)
        context["ti"].xcom_push(key="gold_count", value=gold_count)

    validate_counts = PythonOperator(
        task_id="validate_row_counts",
        python_callable=check_row_counts,
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} pipeline/validate_bronze.py",
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} pipeline/validate_silver.py",
    )

    bronze_to_silver >> validate_bronze >> silver_to_gold >> validate_silver >> validate_counts
