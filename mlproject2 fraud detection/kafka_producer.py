import pandas as pd
import json
import time
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers="localhost:29092", 
                         value_serializer = lambda x: json.dumps(x).encode("utf-8"))

df = pd.read_csv("finsecure_transactions.csv")

print("Starting to stream transactions to Kafka...")

for _, row in df.iterrows():
    transaction = {
        "transaction_id" : row['transaction_id'],
        "transaction_amount" : row['transaction_amount'],
        "merchant_category" : row['merchant_category'],
        "transaction_hour" : row['transaction_hour'],
        "day_of_week" : row['day_of_week'],
        "device_type" : row['device_type'],
        "location_city" : row['location_city'],
        "distance_from_home" : row['distance_from_home'],
        "is_new_device" : int(row['is_new_device']),
        "failed_attempts_last_1hr" : int(row['failed_attempts_last_1hr']),
        "avg_transaction_amount_30d" : row['avg_transaction_amount_30d'],
        "is_fraud" : int(row['is_fraud'])

    }

    producer.send("finsecure.transactions", value = transaction)
    print(f"sent : {row['transaction_id']} | amount : {row['transaction_amount']} | fraud : {row['is_fraud']}")
    time.sleep(0.5)

producer.flush()
print("Done streaming all transactions.")

