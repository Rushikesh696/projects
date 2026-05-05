"""
Synthetic Real-Time Pharma QA Event Data Generator
Serum Institute of India - Quality Performance Metrics
Based on QPM Report (Jan 2025 + Jul 2025)

Generates 6 years of event-level data (2020-2025) with:
- COVID-19 production surge period (2020-2021): 3-4x rates
- 12 event types including full change_control volume
- 30 anomaly windows (COVID emergency + normal operations)
- March spike pattern across unusual_event, quality_risk, material_complaint
  (derived from real PDF page 12-14 data: ~2x normal in March each year)
- All noise preserved:
    * ~7% missing values (product, system, root_cause)
    * ~2% duplicate rows (system glitch)
    * ~5% mislabeled severity (human entry error)
    * Reporting delays on complaints (0-5 days)
    * Night-shift timestamp gaps (23:00-05:00 low activity)

Target: 520,000+ rows
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2025, 12, 31)

# COVID surge period — Serum scaled massively for Covovax
COVID_START = datetime(2020, 4, 1)  # lockdown + emergency production begins
COVID_END = datetime(2021, 12, 31)  # surge normalizes end of 2021

# Monthly rates from QPM Jan 2025 (normal operations)
MONTHLY_RATES = {
    "deviation": 376,
    "complaint": 25,
    "ade": 18,
    "capa": 270,
    "oos": 60,
    "change_control": 3872,  # full real rate from QPM
    "media_fill": 2,
    "vendor_audit": 4,
    "critical_alarm": 15,
    "unusual_event": 80,  # ~80/month normal (PDF: ~80 avg, March spikes to 174)
    "online_material_complaint": 85,  # ~85/month normal (PDF: ~85 avg, March spikes to 180)
    "quality_risk": 80,  # ~80/month normal (PDF: ~80 avg, March spikes to 177)
}

# COVID surge multipliers per event type
COVID_MULTIPLIERS = {
    "deviation": 2.2,  # new processes = more deviations
    "complaint": 1.5,  # new products = more complaints
    "ade": 2.0,  # Covovax ADE monitoring intense
    "capa": 2.0,  # more deviations = more CAPAs
    "oos": 1.8,  # new analytical methods = more OOS
    "change_control": 3.8,  # emergency change controls 4x
    "media_fill": 2.5,  # more fills = more media fills
    "vendor_audit": 1.8,  # new suppliers onboarded
    "critical_alarm": 2.0,  # high-pressure production
    "unusual_event": 2.5,  # COVID chaos = more unusual events
    "online_material_complaint": 2.0,  # new suppliers = more material issues
    "quality_risk": 2.0,  # higher risk during COVID surge
}

SYSTEMS = [
    "Blending",
    "Filling",
    "Lyophilization",
    "Capping",
    "Manufacturing",
    "QC Lab",
    "Warehouse",
]
PRODUCTS = [
    "Covovax",
    "Rota Vaccine",
    "BCG Freeze Dried",
    "BCG Immunotherapy",
    "EPO Vial",
    "EPO PFS",
    "Cy-Tb",
    "Blind Vaccine",
]
ROOT_CAUSES = ["Machine", "Man", "Method", "Material", "Environment"]
SEVERITIES = ["Critical", "Major", "Minor"]
SEV_WEIGHTS = [0.10, 0.25, 0.65]
STATUSES = ["Open", "Closed", "Under Investigation", "Overdue"]

# 30 anomaly windows
ANOMALY_WINDOWS = [
    # 2020 — COVID emergency production ramp-up
    (datetime(2020, 3, 20), datetime(2020, 4, 3), "Manufacturing", 5.0),  # COVID lockdown chaos
    (datetime(2020, 5, 11), datetime(2020, 5, 18), "Filling", 4.5),  # emergency fill line setup
    (datetime(2020, 7, 6), datetime(2020, 7, 13), "QC Lab", 4.2),  # new analytical methods
    (
        datetime(2020, 9, 14),
        datetime(2020, 9, 21),
        "Blending",
        3.8,
    ),  # Covovax bulk production start
    (
        datetime(2020, 11, 23),
        datetime(2020, 11, 30),
        "Lyophilization",
        4.0,
    ),  # lyophilizer capacity issues
    # 2021 — peak COVID vaccine production
    (datetime(2021, 1, 4), datetime(2021, 1, 11), "Filling", 5.5),  # peak Covovax filling
    (datetime(2021, 3, 8), datetime(2021, 3, 15), "Capping", 4.3),  # capping line failures
    (datetime(2021, 5, 17), datetime(2021, 5, 24), "QC Lab", 4.8),  # OOS surge — new batch specs
    (datetime(2021, 7, 26), datetime(2021, 8, 2), "Manufacturing", 4.1),  # scale-up deviations
    (datetime(2021, 10, 4), datetime(2021, 10, 11), "Blending", 3.9),  # blend homogeneity issues
    (datetime(2021, 12, 13), datetime(2021, 12, 20), "Warehouse", 3.5),  # cold chain failures
    # 2022 — post-COVID normalization
    (datetime(2022, 2, 14), datetime(2022, 2, 21), "Blending", 3.5),
    (datetime(2022, 5, 3), datetime(2022, 5, 10), "Filling", 4.0),
    (datetime(2022, 7, 20), datetime(2022, 7, 27), "QC Lab", 3.2),
    (datetime(2022, 10, 1), datetime(2022, 10, 8), "Lyophilization", 3.8),
    (datetime(2022, 12, 5), datetime(2022, 12, 12), "Capping", 4.2),
    # 2023
    (datetime(2023, 1, 16), datetime(2023, 1, 23), "Manufacturing", 3.0),
    (datetime(2023, 4, 10), datetime(2023, 4, 17), "Blending", 3.5),
    (datetime(2023, 6, 22), datetime(2023, 6, 29), "Warehouse", 3.1),
    (datetime(2023, 9, 5), datetime(2023, 9, 12), "Filling", 4.0),
    (datetime(2023, 11, 13), datetime(2023, 11, 20), "QC Lab", 3.6),
    # 2024
    (datetime(2024, 1, 8), datetime(2024, 1, 15), "Capping", 3.3),
    (datetime(2024, 2, 20), datetime(2024, 2, 27), "Lyophilization", 3.0),
    (datetime(2024, 4, 3), datetime(2024, 4, 10), "Blending", 4.8),
    (datetime(2024, 5, 27), datetime(2024, 6, 3), "Manufacturing", 3.4),
    (datetime(2024, 7, 15), datetime(2024, 7, 22), "Capping", 4.5),
    (datetime(2024, 8, 19), datetime(2024, 8, 26), "Filling", 3.7),
    (datetime(2024, 11, 1), datetime(2024, 11, 8), "QC Lab", 3.0),
    # 2025
    (datetime(2025, 1, 20), datetime(2025, 1, 27), "Lyophilization", 3.9),
    (datetime(2025, 3, 10), datetime(2025, 3, 17), "Blending", 4.1),
]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────


def is_covid_period(date: datetime) -> bool:
    return COVID_START <= date <= COVID_END


def get_daily_rate(event_type: str, date: datetime) -> float:
    base = MONTHLY_RATES[event_type] / 30.0
    if is_covid_period(date):
        base *= COVID_MULTIPLIERS[event_type]
    if date.weekday() >= 5:
        base *= 0.4
    return base


def random_timestamp(date: datetime) -> datetime:
    """Random timestamp with night-shift gap noise (23:00-05:00 low activity)."""
    hour_weights = [0.05] * 5 + [0.3] + [1.0] * 16 + [0.3, 0.1]  # 24 values
    hour = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return date.replace(hour=hour, minute=minute, second=second)


def is_in_anomaly_window(date: datetime, system: str):
    for start, end, sys, multiplier in ANOMALY_WINDOWS:
        if start <= date <= end and sys == system:
            return True, multiplier
    return False, 1.0


def add_noise_missing(value, prob=0.07):
    return np.nan if random.random() < prob else value


def noisy_severity():
    base = random.choices(SEVERITIES, weights=SEV_WEIGHTS)[0]
    if random.random() < 0.05:
        return random.choice(SEVERITIES)
    return base


def resolution_days(severity: str, event_type: str) -> float:
    base = {"Critical": 7, "Major": 14, "Minor": 30}.get(severity, 14)
    if random.random() < 0.10:
        return base * random.uniform(2.5, 5.0)
    return base * random.uniform(0.5, 1.8)


def generate_batch_id(product: str, date: datetime) -> str:
    code = product[:3].upper().replace(" ", "")
    return f"{code}{date.strftime('%y%m')}{random.randint(100,999)}"


def base_record(event_type, prefix, date, product=None, system=None):
    product = product or random.choice(PRODUCTS)
    system = system or random.choice(SYSTEMS)
    sev = noisy_severity()
    res = resolution_days(sev, event_type)
    return {
        "timestamp": random_timestamp(date),
        "event_type": event_type,
        "event_id": f"{prefix}-{date.strftime('%Y%m%d')}-{random.randint(1000,9999)}",
        "product": add_noise_missing(product, 0.05),
        "system": add_noise_missing(system, 0.07),
        "root_cause": add_noise_missing(random.choice(ROOT_CAUSES), 0.07),
        "severity": sev,
        "batch_id": add_noise_missing(generate_batch_id(product, date), 0.09),
        "status": random.choices(STATUSES, weights=[0.10, 0.75, 0.10, 0.05])[0],
        "resolution_days": add_noise_missing(round(res, 1), 0.09),
        "closed_timestamp": add_noise_missing(random_timestamp(date + timedelta(days=res)), 0.12),
        "is_anomaly": 0,
        "is_covid_period": int(is_covid_period(date)),
    }


# ─────────────────────────────────────────────
# EVENT GENERATORS
# ─────────────────────────────────────────────


def generate_deviations(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("deviation", date)
        system = random.choice(SYSTEMS)
        in_anomaly, multiplier = is_in_anomaly_window(date, system)
        if in_anomaly:
            rate *= multiplier
        for _ in range(np.random.poisson(rate)):
            rec = base_record("deviation", "DEV", date, system=system)
            rec["is_anomaly"] = int(in_anomaly)
            records.append(rec)
    return records


def generate_complaints(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("complaint", date)
        for _ in range(np.random.poisson(rate)):
            rec = base_record("complaint", "CMP", date)
            delay = random.randint(0, 5)
            rec["timestamp"] = random_timestamp(date - timedelta(days=delay))
            rec["status"] = random.choices(STATUSES, weights=[0.05, 0.80, 0.10, 0.05])[0]
            records.append(rec)
    return records


def generate_ade(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("ade", date)
        for _ in range(np.random.poisson(rate)):
            rec = base_record("ade", "ADE", date, system="QC Lab")
            rec["status"] = random.choices(STATUSES, weights=[0.10, 0.65, 0.15, 0.10])[0]
            records.append(rec)
    return records


def generate_capa(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("capa", date)
        for _ in range(np.random.poisson(rate)):
            rec = base_record("capa", "CAPA", date)
            rec["batch_id"] = np.nan
            rec["status"] = random.choices(STATUSES, weights=[0.08, 0.82, 0.07, 0.03])[0]
            records.append(rec)
    return records


def generate_oos(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("oos", date)
        for _ in range(np.random.poisson(rate)):
            rec = base_record("oos", "OOS", date, system="QC Lab")
            rec["status"] = random.choices(STATUSES, weights=[0.15, 0.65, 0.15, 0.05])[0]
            records.append(rec)
    return records


def generate_change_controls(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("change_control", date)
        for _ in range(np.random.poisson(rate)):
            rec = base_record("change_control", "CC", date)
            rec["batch_id"] = np.nan
            rec["system"] = add_noise_missing("Documentation", 0.05)
            rec["status"] = random.choices(STATUSES, weights=[0.05, 0.85, 0.07, 0.03])[0]
            records.append(rec)
    return records


def generate_media_fill(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("media_fill", date)
        for _ in range(np.random.poisson(rate)):
            product = random.choice(PRODUCTS)
            rec = {
                "timestamp": random_timestamp(date),
                "event_type": "media_fill",
                "event_id": f"MF-{date.strftime('%Y%m%d')}-{random.randint(100,999)}",
                "product": add_noise_missing(product, 0.05),
                "system": add_noise_missing("Filling", 0.05),
                "root_cause": add_noise_missing(random.choice(ROOT_CAUSES), 0.08),
                "severity": random.choices(["Critical", "Major"], weights=[0.6, 0.4])[0],
                "batch_id": add_noise_missing(generate_batch_id(product, date), 0.07),
                "status": random.choices(STATUSES, weights=[0.10, 0.70, 0.15, 0.05])[0],
                "resolution_days": add_noise_missing(round(random.uniform(3, 21), 1), 0.08),
                "closed_timestamp": add_noise_missing(
                    random_timestamp(date + timedelta(days=random.randint(3, 21))), 0.10
                ),
                "is_anomaly": int(random.random() < 0.15),
                "is_covid_period": int(is_covid_period(date)),
            }
            records.append(rec)
    return records


def generate_vendor_audits(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("vendor_audit", date)
        for _ in range(np.random.poisson(rate)):
            sev = noisy_severity()
            rec = {
                "timestamp": random_timestamp(date),
                "event_type": "vendor_audit",
                "event_id": f"VA-{date.strftime('%Y%m%d')}-{random.randint(100,999)}",
                "product": add_noise_missing(random.choice(PRODUCTS), 0.06),
                "system": add_noise_missing("Warehouse", 0.06),
                "root_cause": add_noise_missing(random.choice(ROOT_CAUSES), 0.08),
                "severity": sev,
                "batch_id": np.nan,
                "status": random.choices(STATUSES, weights=[0.15, 0.65, 0.15, 0.05])[0],
                "resolution_days": add_noise_missing(
                    round(resolution_days(sev, "vendor_audit"), 1), 0.09
                ),
                "closed_timestamp": add_noise_missing(
                    random_timestamp(date + timedelta(days=random.randint(5, 60))), 0.12
                ),
                "is_anomaly": int(random.random() < 0.10),
                "is_covid_period": int(is_covid_period(date)),
            }
            records.append(rec)
    return records


def generate_critical_alarms(dates):
    records = []
    for date in dates:
        rate = get_daily_rate("critical_alarm", date)
        for _ in range(np.random.poisson(rate)):
            system = random.choices(
                ["QC Lab", "Filling", "Lyophilization", "Blending"],
                weights=[0.35, 0.25, 0.25, 0.15],
            )[0]
            in_anomaly, _ = is_in_anomaly_window(date, system)
            rec = {
                "timestamp": random_timestamp(date),
                "event_type": "critical_alarm",
                "event_id": f"CA-{date.strftime('%Y%m%d')}-{random.randint(100,999)}",
                "product": add_noise_missing(random.choice(PRODUCTS), 0.07),
                "system": add_noise_missing(system, 0.05),
                "root_cause": add_noise_missing(random.choice(ROOT_CAUSES), 0.08),
                "severity": random.choices(["Critical", "Major"], weights=[0.55, 0.45])[0],
                "batch_id": add_noise_missing(
                    generate_batch_id(random.choice(PRODUCTS), date), 0.10
                ),
                "status": random.choices(STATUSES, weights=[0.20, 0.60, 0.15, 0.05])[0],
                "resolution_days": add_noise_missing(round(random.uniform(0.5, 7), 1), 0.07),
                "closed_timestamp": add_noise_missing(
                    random_timestamp(date + timedelta(days=random.randint(1, 7))), 0.10
                ),
                "is_anomaly": int(in_anomaly or random.random() < 0.08),
                "is_covid_period": int(is_covid_period(date)),
            }
            records.append(rec)
    return records


def generate_unusual_events(dates):
    """
    Unusual events — unplanned incidents not captured by deviation system.
    Key pattern from PDF page 12: March spikes to ~174 (2x normal ~80).
    Correlated with quality_risk and material_complaint spikes — leading indicator.
    """
    records = []
    for date in dates:
        base_rate = get_daily_rate("unusual_event", date)
        # March spike: ~2.2x normal (real PDF: 174 vs ~80 avg = 2.17x)
        if date.month == 3:
            base_rate *= 2.2
        # February slight elevation (PDF: 77 → building toward March)
        elif date.month == 2:
            base_rate *= 1.1

        for _ in range(np.random.poisson(base_rate)):
            system = random.choice(SYSTEMS)
            in_anomaly, _ = is_in_anomaly_window(date, system)
            sev = noisy_severity()
            res = resolution_days(sev, "unusual_event")
            rec = {
                "timestamp": random_timestamp(date),
                "event_type": "unusual_event",
                "event_id": f"UE-{date.strftime('%Y%m%d')}-{random.randint(1000,9999)}",
                "product": add_noise_missing(random.choice(PRODUCTS), 0.06),
                "system": add_noise_missing(system, 0.06),
                "root_cause": add_noise_missing(random.choice(ROOT_CAUSES), 0.08),
                "severity": sev,
                "batch_id": add_noise_missing(
                    generate_batch_id(random.choice(PRODUCTS), date), 0.10
                ),
                "status": random.choices(STATUSES, weights=[0.12, 0.72, 0.11, 0.05])[0],
                "resolution_days": add_noise_missing(round(res, 1), 0.09),
                "closed_timestamp": add_noise_missing(
                    random_timestamp(date + timedelta(days=res)), 0.12
                ),
                # Anomalous if March spike OR within existing anomaly window
                "is_anomaly": int(date.month == 3 or in_anomaly),
                "is_covid_period": int(is_covid_period(date)),
            }
            records.append(rec)
    return records


def generate_online_material_complaints(dates):
    """
    Online material complaints — supplier/material facing complaints.
    Different from product complaints (patient facing).
    Key pattern from PDF page 14: March spikes to ~180 (2.1x normal ~85).
    Leading indicator: material complaint spike → deviation spike 2-4 weeks later.
    """
    records = []
    for date in dates:
        base_rate = get_daily_rate("online_material_complaint", date)
        # March spike: ~2.1x normal (real PDF: 180 vs ~85 avg)
        if date.month == 3:
            base_rate *= 2.1
        elif date.month == 2:
            base_rate *= 1.08

        for _ in range(np.random.poisson(base_rate)):
            sev = noisy_severity()
            res = resolution_days(sev, "online_material_complaint")
            # Reporting delay: material complaints often logged 1-3 days late
            delay = random.randint(0, 3)
            rec = {
                "timestamp": random_timestamp(date - timedelta(days=delay)),
                "event_type": "online_material_complaint",
                "event_id": f"MC-{date.strftime('%Y%m%d')}-{random.randint(1000,9999)}",
                "product": add_noise_missing(random.choice(PRODUCTS), 0.06),
                "system": add_noise_missing("Warehouse", 0.07),
                "root_cause": add_noise_missing(random.choice(ROOT_CAUSES), 0.08),
                "severity": sev,
                "batch_id": add_noise_missing(
                    generate_batch_id(random.choice(PRODUCTS), date), 0.10
                ),
                "status": random.choices(STATUSES, weights=[0.10, 0.75, 0.10, 0.05])[0],
                "resolution_days": add_noise_missing(round(res, 1), 0.09),
                "closed_timestamp": add_noise_missing(
                    random_timestamp(date + timedelta(days=res)), 0.13
                ),
                "is_anomaly": int(date.month == 3),
                "is_covid_period": int(is_covid_period(date)),
            }
            records.append(rec)
    return records


def generate_quality_risk(dates):
    """
    Quality Risk Assessment events — open/closed risk items tracked monthly.
    Key pattern from PDF page 13: March spikes to ~177 (2.2x normal ~80).
    Acts as leading indicator — risk score rises before quality events spike.
    In dataset: each row = one risk item opened or closed on that day.
    """
    records = []
    for date in dates:
        base_rate = get_daily_rate("quality_risk", date)
        # March spike: ~2.2x normal (real PDF: 177 vs ~80 avg)
        if date.month == 3:
            base_rate *= 2.2
        elif date.month == 2:
            base_rate *= 1.1

        for _ in range(np.random.poisson(base_rate)):
            sev = noisy_severity()
            res = resolution_days(sev, "quality_risk")
            rec = {
                "timestamp": random_timestamp(date),
                "event_type": "quality_risk",
                "event_id": f"QR-{date.strftime('%Y%m%d')}-{random.randint(1000,9999)}",
                "product": add_noise_missing(random.choice(PRODUCTS), 0.07),
                "system": add_noise_missing(random.choice(SYSTEMS), 0.07),
                "root_cause": add_noise_missing(random.choice(ROOT_CAUSES), 0.08),
                "severity": sev,
                "batch_id": np.nan,  # risk items not tied to single batch
                "status": random.choices(STATUSES, weights=[0.15, 0.70, 0.10, 0.05])[0],
                "resolution_days": add_noise_missing(round(res, 1), 0.09),
                "closed_timestamp": add_noise_missing(
                    random_timestamp(date + timedelta(days=res)), 0.12
                ),
                "is_anomaly": int(date.month == 3),
                "is_covid_period": int(is_covid_period(date)),
            }
            records.append(rec)
    return records


# ─────────────────────────────────────────────
# DUPLICATE INJECTOR (~2%)
# ─────────────────────────────────────────────


def inject_duplicates(df, rate=0.02):
    n_dups = int(len(df) * rate)
    dup_rows = df.sample(n=n_dups, random_state=42)
    return pd.concat([df, dup_rows], ignore_index=True)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────


def main():
    print("Generating date range...")
    all_dates = [START_DATE + timedelta(days=i) for i in range((END_DATE - START_DATE).days + 1)]
    print(f"Total days  : {len(all_dates)}")
    print(f"COVID period: {COVID_START.date()} → {COVID_END.date()}")

    generators = [
        ("deviations", generate_deviations),
        ("complaints", generate_complaints),
        ("ADEs", generate_ade),
        ("CAPAs", generate_capa),
        ("OOS", generate_oos),
        ("change controls", generate_change_controls),
        ("media fills", generate_media_fill),
        ("vendor audits", generate_vendor_audits),
        ("critical alarms", generate_critical_alarms),
        ("unusual events", generate_unusual_events),
        ("online material complaints", generate_online_material_complaints),
        ("quality risk items", generate_quality_risk),
    ]

    all_records = []
    for name, fn in generators:
        print(f"Generating {name}...")
        records = fn(all_dates)
        print(f"  {len(records):,} records")
        all_records.extend(records)

    print("\nCombining all events...")
    df = pd.DataFrame(all_records)

    print("Injecting duplicates (~2%)...")
    df = inject_duplicates(df, rate=0.02)

    print("Sorting by timestamp...")
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Derived time features
    df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
    df["day_of_week"] = pd.to_datetime(df["timestamp"]).dt.dayofweek
    df["month"] = pd.to_datetime(df["timestamp"]).dt.month
    df["year"] = pd.to_datetime(df["timestamp"]).dt.year
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

    out_path = "/home/rushikesh/Music/serum_qa_realtime_raw.csv"
    df.to_csv(out_path, index=False)

    print(f"\n{'='*55}")
    print(f"DONE — saved to: {out_path}")
    print(f"{'='*55}")
    print(f"Total records     : {len(df):,}")
    print(f"Date range        : {START_DATE.date()} → {END_DATE.date()}")
    print(f"Columns           : {list(df.columns)}")
    print(f"\nEvent type distribution:")
    print(df["event_type"].value_counts().to_string())
    print(f"\nAnomalous events  : {df['is_anomaly'].sum():,} ({df['is_anomaly'].mean()*100:.2f}%)")
    print(
        f"COVID period rows : {df['is_covid_period'].sum():,} ({df['is_covid_period'].mean()*100:.1f}%)"
    )
    print(f"\nMissing values:")
    missing = df.isnull().sum()
    print(missing[missing > 0].to_string())
    print(f"\nAnomaly windows   : {len(ANOMALY_WINDOWS)}")
    print(
        f"File size         : {round(df.memory_usage(deep=True).sum() / 1024**2, 1)} MB in memory"
    )


if __name__ == "__main__":
    main()
