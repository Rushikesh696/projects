import json
import uuid
import time
import random
import argparse
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "pharma.sales.transactions"

CHANNELS = ["Hospital", "Clinic", "Retail"]
SPEED_MODES = {"fast": 0.05, "normal": 0.5, "slow": 2.0}


def load_master_data():
    base = "data/raw"
    drugs = pd.read_csv(f"{base}/drug_master.csv")
    regions = pd.read_csv(f"{base}/region_master.csv")
    events = pd.read_csv(f"{base}/seasonal_events.csv")
    events["start_date"] = pd.to_datetime(events["start_date"])
    events["end_date"] = pd.to_datetime(events["end_date"])
    return drugs, regions, events


def get_active_events(events_df, current_date):
    dt = pd.Timestamp(current_date)
    return events_df[(events_df["start_date"] <= dt) & (events_df["end_date"] >= dt)]


def compute_demand_multiplier(active_events, drug_category, region_zone):
    multiplier = 1.0
    for _, evt in active_events.iterrows():
        zones = evt["affected_zones"].split("|")
        categories = evt["affected_drug_categories"].split("|")
        if region_zone in zones and drug_category in categories:
            impact = evt["demand_impact"]
            if impact == "High":
                multiplier *= random.uniform(1.4, 1.8)
            elif impact == "Medium":
                multiplier *= random.uniform(1.1, 1.4)
            else:
                multiplier *= random.uniform(1.0, 1.1)
    return multiplier


def generate_transaction(drugs, regions, events, sim_date=None):
    drug = drugs.sample(1).iloc[0]
    region = regions.sample(1).iloc[0]
    channel = random.choice(CHANNELS)

    if sim_date is None:
        sim_date = datetime.now() - timedelta(days=random.randint(0, 365))

    active_events = get_active_events(events, sim_date)
    multiplier = compute_demand_multiplier(active_events, drug["category"], region["zone"])

    base_qty = random.randint(10, 500)
    quantity_sold = max(1, int(base_qty * multiplier))
    unit_price = float(drug["unit_price"])
    returns = random.randint(0, max(1, int(quantity_sold * 0.05)))
    revenue = round((quantity_sold - returns) * unit_price, 2)

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": sim_date.isoformat(),
        "drug_id": drug["drug_id"],
        "drug_name": drug["drug_name"],
        "drug_category": drug["category"],
        "drug_type": drug["drug_type"],
        "region_id": region["region_id"],
        "region_name": region["region_name"],
        "zone": region["zone"],
        "tier": region["tier"],
        "channel": channel,
        "quantity_sold": quantity_sold,
        "unit_price": unit_price,
        "returns": returns,
        "revenue": revenue,
        "cold_chain_required": bool(drug["cold_chain_required"]),
        "is_generic": bool(drug["is_generic"]),
        "manufacturer": drug["manufacturer"],
        "ingestion_source": "kafka_producer",
        "ingestion_timestamp": datetime.now().isoformat(),
    }
    return transaction


def run_producer(speed="normal", batch=False, batch_size=1000):
    drugs, regions, events = load_master_data()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    delay = SPEED_MODES.get(speed, 0.5)
    print(f"Starting Kafka producer in '{speed}' mode. Topic: {TOPIC}")

    count = 0
    sim_date = datetime.now() - timedelta(days=730)

    try:
        while True:
            txn = generate_transaction(drugs, regions, events, sim_date)
            producer.send(TOPIC, key=txn["transaction_id"], value=txn)
            count += 1
            sim_date += timedelta(minutes=random.randint(1, 60))

            if count % 100 == 0:
                print(f"Sent {count} transactions...")
                producer.flush()

            if batch and count >= batch_size:
                print(f"Batch complete. Sent {count} transactions.")
                break

            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\nProducer stopped. Total sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PharmaCast Kafka Producer")
    parser.add_argument("--speed", choices=["fast", "normal", "slow"], default="normal")
    parser.add_argument("--batch", action="store_true", help="Run in batch mode")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    run_producer(speed=args.speed, batch=args.batch, batch_size=args.batch_size)
