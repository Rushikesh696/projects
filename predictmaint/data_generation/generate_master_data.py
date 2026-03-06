import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

MACHINE_TYPES = ["Pump", "Compressor", "Motor", "Turbine", "Conveyor"]
LOCATIONS = ["Plant-A", "Plant-B", "Plant-C", "Plant-D"]
MANUFACTURERS = ["Siemens", "ABB", "Bosch", "GE", "Honeywell", "Emerson", "Rockwell"]
CRITICALITY = ["High", "Medium", "Low"]

# Sensor thresholds per machine type: {sensor: (normal_min, normal_max, critical_max)}
THRESHOLDS = {
    "Pump":       {"temperature": (60, 85, 105), "vibration": (0.5, 3.0, 6.0), "pressure": (2.0, 8.0, 12.0), "rpm": (1200, 1800, 2200), "voltage": (210, 240, 260)},
    "Compressor": {"temperature": (50, 80, 100), "vibration": (0.3, 2.5, 5.5), "pressure": (5.0, 15.0, 20.0), "rpm": (900, 1500, 1800), "voltage": (210, 240, 260)},
    "Motor":      {"temperature": (40, 75, 95),  "vibration": (0.2, 2.0, 5.0), "pressure": (1.0, 4.0, 7.0),  "rpm": (1500, 3000, 3600), "voltage": (200, 240, 260)},
    "Turbine":    {"temperature": (80, 120, 150),"vibration": (1.0, 4.0, 8.0), "pressure": (10.0, 30.0, 40.0),"rpm": (3000, 5000, 6000), "voltage": (220, 240, 260)},
    "Conveyor":   {"temperature": (30, 60, 80),  "vibration": (0.1, 1.5, 4.0), "pressure": (0.5, 2.0, 4.0),  "rpm": (100, 300, 500),   "voltage": (200, 240, 260)},
}


def generate_machine_master(n=50):
    machines = []
    for i in range(1, n + 1):
        mtype = random.choice(MACHINE_TYPES)
        install_date = datetime(2015, 1, 1) + timedelta(days=random.randint(0, 2000))
        age_years = round((datetime.now() - install_date).days / 365, 1)
        thresholds = THRESHOLDS[mtype]
        machines.append({
            "machine_id": f"M{i:03d}",
            "machine_name": f"{mtype}_{i:03d}",
            "machine_type": mtype,
            "location": random.choice(LOCATIONS),
            "manufacturer": random.choice(MANUFACTURERS),
            "installation_date": install_date.strftime("%Y-%m-%d"),
            "age_years": age_years,
            "criticality": random.choice(CRITICALITY),
            "temp_min": thresholds["temperature"][0],
            "temp_max": thresholds["temperature"][1],
            "temp_critical": thresholds["temperature"][2],
            "vibration_min": thresholds["vibration"][0],
            "vibration_max": thresholds["vibration"][1],
            "vibration_critical": thresholds["vibration"][2],
            "pressure_min": thresholds["pressure"][0],
            "pressure_max": thresholds["pressure"][1],
            "pressure_critical": thresholds["pressure"][2],
            "rpm_min": thresholds["rpm"][0],
            "rpm_max": thresholds["rpm"][1],
            "rpm_critical": thresholds["rpm"][2],
            "voltage_min": thresholds["voltage"][0],
            "voltage_max": thresholds["voltage"][1],
            "voltage_critical": thresholds["voltage"][2],
        })
    df = pd.DataFrame(machines)
    df.to_csv(os.path.join(OUTPUT_DIR, "machine_master.csv"), index=False)
    print(f"Generated machine_master.csv with {len(df)} records.")
    return df


def generate_failure_history(machines_df, n_failures=200):
    failures = []
    for i in range(n_failures):
        machine = machines_df.sample(1).iloc[0]
        failure_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 900))
        failures.append({
            "failure_id": f"F{i+1:04d}",
            "machine_id": machine["machine_id"],
            "machine_type": machine["machine_type"],
            "failure_date": failure_date.strftime("%Y-%m-%d"),
            "failure_type": random.choice(["Bearing Failure", "Overheating", "Pressure Leak", "Electrical Fault", "Mechanical Wear"]),
            "downtime_hours": round(random.uniform(1.0, 72.0), 1),
            "repair_cost_usd": round(random.uniform(500, 50000), 2),
            "root_cause": random.choice(["Wear & Tear", "Overload", "Lubrication Failure", "Contamination", "Misalignment"]),
        })
    df = pd.DataFrame(failures)
    df.to_csv(os.path.join(OUTPUT_DIR, "failure_history.csv"), index=False)
    print(f"Generated failure_history.csv with {len(df)} records.")
    return df


if __name__ == "__main__":
    machines = generate_machine_master()
    generate_failure_history(machines)
    print("Master data generation complete.")
