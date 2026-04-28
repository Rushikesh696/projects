"""
Bronze Layer Validation — Great Expectations
Validates schema and critical constraints on bronze.qa_events.

Install: pip install great-expectations
Run:     python pipeline/validate_bronze.py
"""
import json
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
import great_expectations as ge
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_PATH = BASE_DIR / "logs" / "validate_bronze.log"
REPORT_PATH = BASE_DIR / "logs" / "ge_bronze_report.json"
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

VALID_EVENT_TYPES = [
    "deviation", "complaint", "ade", "capa", "oos",
    "change_control", "media_fill", "vendor_audit",
    "critical_alarm", "unusual_event", "online_material_complaint", "quality_risk",
]

VALID_SEVERITIES = ["Critical", "Major", "Minor"]

REQUIRED_COLUMNS = [
    "event_id", "timestamp", "event_type", "product", "system",
    "root_cause", "severity", "batch_id", "status", "resolution_days",
    "is_anomaly", "is_covid_period", "hour", "day_of_week",
    "month", "year", "is_weekend",
]


def load_bronze_sample() -> pd.DataFrame:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    try:
        # Sample 10K rows for fast validation — full scan on demand
        df = pd.read_sql(
            "SELECT * FROM bronze.qa_events ORDER BY id LIMIT 10000",
            conn,
        )
        log.info(f"Loaded {len(df):,} rows from bronze.qa_events for validation.")
        return df
    finally:
        conn.close()


def run_validations(df: pd.DataFrame) -> dict:
    gdf = ge.from_pandas(df)
    results = []

    # ── Schema: required columns exist ──────────────────────────────────────
    for col in REQUIRED_COLUMNS:
        r = gdf.expect_column_to_exist(col)
        results.append(("schema", f"column_exists:{col}", r["success"]))

    # ── event_id: not null, unique ───────────────────────────────────────────
    results.append(("critical", "event_id_not_null",
                    gdf.expect_column_values_to_not_be_null("event_id")["success"]))
    results.append(("critical", "event_id_unique",
                    gdf.expect_column_values_to_be_unique("event_id")["success"]))

    # ── timestamp: not null ──────────────────────────────────────────────────
    results.append(("critical", "timestamp_not_null",
                    gdf.expect_column_values_to_not_be_null("timestamp")["success"]))

    # ── event_type: in valid set ─────────────────────────────────────────────
    results.append(("quality", "event_type_valid_set",
                    gdf.expect_column_values_to_be_in_set("event_type", VALID_EVENT_TYPES)["success"]))

    # ── severity: in valid set (allows nulls — Silver handles imputation) ───
    results.append(("quality", "severity_valid_set",
                    gdf.expect_column_values_to_be_in_set(
                        "severity", VALID_SEVERITIES, mostly=0.90
                    )["success"]))

    # ── is_anomaly: only 0 or 1 ─────────────────────────────────────────────
    results.append(("critical", "is_anomaly_binary",
                    gdf.expect_column_values_to_be_in_set("is_anomaly", [0, 1])["success"]))

    # ── Row count > 0 ────────────────────────────────────────────────────────
    results.append(("critical", "row_count_positive",
                    gdf.expect_table_row_count_to_be_between(min_value=1)["success"]))

    # ── Null rates: product/system/root_cause allowed up to 10% ─────────────
    for col in ["product", "system", "root_cause"]:
        r = gdf.expect_column_values_to_not_be_null(col, mostly=0.90)
        results.append(("quality", f"{col}_null_rate_lt_10pct", r["success"]))

    return results


def evaluate_results(results: list) -> bool:
    passed = [r for r in results if r[2]]
    failed = [r for r in results if not r[2]]
    critical_failures = [r for r in failed if r[0] == "critical"]

    log.info(f"Validation results — Passed: {len(passed)}, Failed: {len(failed)}")

    for tier, name, _ in failed:
        log.warning(f"  FAILED [{tier}] {name}")

    report = {
        "layer": "bronze",
        "run_at": datetime.now().isoformat(),
        "total": len(results),
        "passed": len(passed),
        "failed": len(failed),
        "critical_failures": len(critical_failures),
        "failures": [{"tier": t, "name": n} for t, n, _ in failed],
    }
    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    log.info(f"Report saved to {REPORT_PATH}")

    return len(critical_failures) == 0


def main():
    log.info("Starting Bronze layer validation...")
    df = load_bronze_sample()
    results = run_validations(df)
    passed = evaluate_results(results)

    if not passed:
        log.error("Bronze validation FAILED — critical expectations not met. Halting pipeline.")
        sys.exit(1)

    log.info("Bronze validation PASSED.")


if __name__ == "__main__":
    main()
