from pyspark.sql import SparkSession                                                                                                                                                
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType                                                                                           
import joblib 
import pandas as pd
import numpy as np
import os

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("transaction_amount", FloatType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_hour", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("location_city", StringType(), True),
    StructField("distance_from_home", FloatType(), True),
    StructField("is_new_device", IntegerType(), True),
    StructField("failed_attempts_last_1hr", IntegerType(), True),
    StructField("avg_transaction_amount_30d", FloatType(), True),
    StructField("is_fraud", IntegerType(), True)
])

spark = SparkSession.builder.appName(
    "Finsecure fraud detection").config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0").getOrCreate()
    

spark.sparkContext.setLogLevel("ERROR")

le = joblib.load("label_encoder.pkl")
city_mapping = {city : int(idx) for idx, city in enumerate(le.classes_)}

model = joblib.load("fraud_model.pkl")

model_features = model.get_booster().feature_names

def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return 
    pandas_df = batch_df.toPandas()
    meta = pandas_df[["transaction_amount", "merchant_category", "device_type", "location_city"]].copy()
    for col_name in model_features:
        if col_name not in pandas_df.columns:
            pandas_df[col_name] = 0
    pandas_df = pandas_df[model_features]
    probs = model.predict_proba(pandas_df)[:, 1]
    decisions = np.where(probs >= 0.7, "block", np.where(probs >= 0.3, "review", "approve"))
    meta["fraud_probability"] = probs.round(4)
    meta["decision"] = decisions
    meta.to_csv("predictions.csv", mode="a", header=not os.path.exists("predictions.csv"), index=False)


df_raw = spark.readStream.format(
    "kafka").option(
        "kafka.bootstrap.servers", "localhost:29092").option(
            "subscribe", "finsecure.transactions").option(
                 "startingOffsets", "latest").load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str").select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

merchant_categories = ["fuel", "electronics", "restaurant", "grocery", 
                       "clothing", "jewelry", "pharmacy", "travel", 
                       "utilities", "entertainment"]
for cat in merchant_categories:
    df_parsed = df_parsed.withColumn(
        f"merchant_category_{cat}", when(col("merchant_category") == cat, 1).otherwise(0)
    )

device_types = ["mobile", "pos", "web"]
for device in device_types:
    df_parsed = df_parsed.withColumn(
        f"device_type_{device}", when(col("device_type") == device, 1).otherwise(0)
    )

city_encoded = col("location_city")
for city, idx in city_mapping.items():
    city_encoded = when(col("location_city") == city, idx).otherwise(city_encoded)

df_parsed = df_parsed.withColumn("location_city", city_encoded.cast(IntegerType()))

df_processed = df_parsed.drop("transaction_id", "is_fraud")

df_processed.writeStream.foreachBatch(process_batch).start().awaitTermination()


