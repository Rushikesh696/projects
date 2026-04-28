"""
Silver Layer Validation — Great Expectations
Validates cleaned data quality on silver.qa_events_clean.

Install: pip install great-expectations
Run:     python pipeline/validate_silver.py
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
LOG_PATH = BASE_DIR / "logs" / "validate_silver.log"
REPORT_PATH = BASE_DIR / "logs" / "ge_silver_report.json"
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

VALID_SEVERITIES = ["Critical", "Major", "Minor"]


def load_silver_sample() -> pd.DataFrame:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    try:
        df = pd.read_sql(
            "SELECT * FROM silver.qa_events_clean ORDER BY id LIMIT 10000",
            conn,
        )
        log.info(f"Loaded {len(df):,} rows from silver.qa_events_clean for validation.")
        return df
    finally:
        conn.close()


def get_full_row_count() -> int:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.qa_events_clean")
            return cur.fetchone()[0]
    finally:
        conn.close()


def run_validations(df: pd.DataFrame) -> list:
    gdf = ge.from_pandas(df)
    results = []

    # ── No NULLs in imputed columns (Silver must have fixed these) ───────────
    for col in ["product", "system", "root_cause"]:
        r = gdf.expect_column_values_to_not_be_null(col)
        results.append(("critical", f"{col}_not_null_after_imputation", r["success"]))

    # ── event_id: not null ───────────────────────────────────────────────────
    results.append(("critical", "event_id_not_null",
                    gdf.expect_column_values_to_not_be_null("event_id")["success"]))

    # ── timestamp: not null ──────────────────────────────────────────────────
    results.append(("critical", "timestamp_not_null",
                    gdf.expect_column_values_to_not_be_null("timestamp")["success"]))

    # ── severity: ONLY valid values (Silver normalized these) ───────────────
    results.append(("critical", "severity_only_valid_values",
                    gdf.expect_column_values_to_be_in_set("severity", VALID_SEVERITIES)["success"]))

    # ── is_anomaly: only 0 or 1 ─────────────────────────────────────────────
    results.append(("critical", "is_anomaly_binary",
                    gdf.expect_column_values_to_be_in_set("is_anomaly", [0, 1])["success"]))

    # ── Anomaly rate: between 0.1% and 5% ───────────────────────────────────
    anomaly_rate = df["is_anomaly"].mean()
    anomaly_rate_ok = 0.001 <= anomaly_rate <= 0.05
    results.append(("quality", f"anomaly_rate_in_range (actual={anomaly_rate:.4%})", anomaly_rate_ok))

    # ── resolution_days: non-negative where not null ─────────────────────────
    results.append(("quality", "resolution_days_non_negative",
                    gdf.expect_column_values_to_be_between(
                        "resolution_days", min_value=0, mostly=0.99
                    )["success"]))

    # ── hour: 0–23 ───────────────────────────────────────────────────────────
    results.append(("quality", "hour_in_range_0_23",
                    gdf.expect_column_values_to_be_between("hour", min_value=0, max_value=23)["success"]))

    # ── month: 1–12 ──────────────────────────────────────────────────────────
    results.append(("quality", "month_in_range_1_12",
                    gdf.expect_column_values_to_be_between("month", min_value=1, max_value=12)["success"]))

    # ── year: 2020–2025 ──────────────────────────────────────────────────────
    results.append(("quality", "year_in_range_2020_2025",
                    gdf.expect_column_values_to_be_between("year", min_value=2020, max_value=2025)["success"]))

    return results


def evaluate_results(results: list, full_row_count: int) -> bool:
    passed = [r for r in results if r[2]]
    failed = [r for r in results if not r[2]]
    critical_failures = [r for r in failed if r[0] == "critical"]

    log.info(f"Full silver row count: {full_row_count:,}")
    log.info(f"Validation results — Passed: {len(passed)}, Failed: {len(failed)}")

    for tier, name, _ in failed:
        log.warning(f"  FAILED [{tier}] {name}")

    # Warn if row count dropped more than 5% vs expected bronze (~439K)
    if full_row_count < 400_000:
        log.warning(f"Row count {full_row_count:,} is lower than expected (>400K) — check dedup logic.")

    report = {
        "layer": "silver",
        "run_at": datetime.now().isoformat(),
        "full_row_count": full_row_count,
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
    log.info("Starting Silver layer validation...")
    df = load_silver_sample()
    full_row_count = get_full_row_count()
    results = run_validations(df)
    passed = evaluate_results(results, full_row_count)

    if not passed:
        log.error("Silver validation FAILED — critical expectations not met. Halting pipeline.")
        sys.exit(1)

    log.info("Silver validation PASSED.")


if __name__ == "__main__":
    main()
