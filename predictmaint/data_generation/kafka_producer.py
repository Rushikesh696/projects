import json
import uuid
import time
import random
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "iot.sensor.readings"

random.seed()
np.random.seed()


def load_machines():
    return pd.read_csv("data/raw/machine_master.csv")


def normal_reading(low, high):
    return round(random.uniform(low, high), 3)


def degraded_reading(low, high, critical, factor):
    """Gradually push reading towards critical value."""
    base = random.uniform(low, high)
    degraded = base + (critical - high) * factor
    return round(min(degraded + random.uniform(-1, 1), critical * 1.05), 3)


def generate_sensor_reading(machine, degradation_factor=0.0):
    is_degrading = degradation_factor > 0.3
    will_fail = degradation_factor > 0.8

    def read(low, high, crit):
        if degradation_factor < 0.3:
            return normal_reading(low, high)
        return degraded_reading(low, high, crit, degradation_factor)

    temperature = read(machine["temp_min"], machine["temp_max"], machine["temp_critical"])
    vibration   = read(machine["vibration_min"], machine["vibration_max"], machine["vibration_critical"])
    pressure    = read(machine["pressure_min"], machine["pressure_max"], machine["pressure_critical"])
    rpm         = read(machine["rpm_min"], machine["rpm_max"], machine["rpm_critical"])
    voltage     = normal_reading(machine["voltage_min"], machine["voltage_max"])

    return {
        "reading_id": str(uuid.uuid4()),
        "machine_id": machine["machine_id"],
        "machine_name": machine["machine_name"],
        "machine_type": machine["machine_type"],
        "location": machine["location"],
        "timestamp": datetime.now().isoformat(),
        "temperature": temperature,
        "vibration": vibration,
        "pressure": pressure,
        "rpm": rpm,
        "voltage": voltage,
        "degradation_factor": round(degradation_factor, 3),
        "is_degrading": is_degrading,
        "will_fail_in_4hrs": will_fail,
        "ingestion_source": "iot_simulator",
        "ingestion_timestamp": datetime.now().isoformat(),
    }


def run_producer(speed="normal", batch=False, batch_size=5000):
    machines = load_machines()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    delays = {"fast": 0.05, "normal": 0.3, "slow": 1.0}
    delay = delays.get(speed, 0.3)

    # Track degradation per machine (random machines degrade over time)
    degradation = {row["machine_id"]: 0.0 for _, row in machines.iterrows()}
    degrading_machines = set(machines.sample(frac=0.2)["machine_id"].tolist())

    print(f"Starting IoT producer in '{speed}' mode. Topic: {TOPIC}")
    print(f"Degrading machines: {degrading_machines}")

    count = 0
    try:
        while True:
            for _, machine in machines.iterrows():
                mid = machine["machine_id"]
                if mid in degrading_machines:
                    degradation[mid] = min(degradation[mid] + random.uniform(0.001, 0.005), 1.0)
                    if degradation[mid] >= 1.0:
                        degradation[mid] = 0.0  # reset after failure

                reading = generate_sensor_reading(machine, degradation[mid])
                producer.send(TOPIC, key=reading["reading_id"], value=reading)
                count += 1

            if count % 500 == 0:
                print(f"Sent {count} readings...")
                producer.flush()

            if batch and count >= batch_size:
                print(f"Batch complete. Sent {count} readings.")
                break

            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\nProducer stopped. Total sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PredictMaint IoT Kafka Producer")
    parser.add_argument("--speed", choices=["fast", "normal", "slow"], default="normal")
    parser.add_argument("--batch", action="store_true")
    parser.add_argument("--batch-size", type=int, default=5000)
    args = parser.parse_args()
    run_producer(speed=args.speed, batch=args.batch, batch_size=args.batch_size)
