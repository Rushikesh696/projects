from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, when, lit, current_timestamp,
    trim, upper, expr, broadcast
)
from pyspark.sql.types import IntegerType, FloatType

BRONZE_PATH = "data/bronze/sales_transactions"
SILVER_PATH = "data/silver/sales_enriched"
CHECKPOINT_PATH = "data/checkpoints/silver"
RAW_PATH = "data/raw"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("PharmaCast-Silver-Transform")
        .getOrCreate()
    )


def load_master_data(spark):
    drug_master = spark.read.csv(f"{RAW_PATH}/drug_master.csv", header=True, inferSchema=True)
    region_master = spark.read.csv(f"{RAW_PATH}/region_master.csv", header=True, inferSchema=True)
    seasonal_events = spark.read.csv(f"{RAW_PATH}/seasonal_events.csv", header=True, inferSchema=True)
    seasonal_events = seasonal_events.withColumn("start_date", to_date(col("start_date"))) \
                                     .withColumn("end_date", to_date(col("end_date")))
    return drug_master, region_master, seasonal_events


def clean_and_enrich(batch_df, batch_id, drug_master, region_master, seasonal_events):
    # Type casting
    df = batch_df \
        .withColumn("transaction_ts", to_timestamp(col("timestamp"))) \
        .withColumn("transaction_date", to_date(col("timestamp"))) \
        .withColumn("quantity_sold", col("quantity_sold").cast(IntegerType())) \
        .withColumn("unit_price", col("unit_price").cast(FloatType())) \
        .withColumn("returns", col("returns").cast(IntegerType())) \
        .withColumn("revenue", col("revenue").cast(FloatType()))

    # Null handling
    df = df.dropna(subset=["transaction_id", "drug_id", "region_id", "quantity_sold"])

    # Deduplication
    df = df.dropDuplicates(["transaction_id"])

    # Trim strings
    for c in ["drug_name", "region_name", "zone", "tier", "channel", "manufacturer"]:
        df = df.withColumn(c, trim(col(c)))

    # Join drug master
    drug_cols = drug_master.select("drug_id", "shelf_life_months", "cold_chain_required", "is_generic")
    df = df.join(broadcast(drug_cols), on="drug_id", how="left")

    # Join region master
    region_cols = region_master.select("region_id", "population_proxy", "num_hospitals", "num_clinics")
    df = df.join(broadcast(region_cols), on="region_id", how="left")

    # Event day flag
    events_broadcast = seasonal_events.select("start_date", "end_date", "demand_impact")
    event_dates = events_broadcast.withColumnRenamed("start_date", "evt_start") \
                                  .withColumnRenamed("end_date", "evt_end")
    df = df.join(
        event_dates,
        (col("transaction_date") >= col("evt_start")) & (col("transaction_date") <= col("evt_end")),
        how="left"
    ).withColumn("is_event_day", when(col("demand_impact").isNotNull(), True).otherwise(False)) \
     .drop("evt_start", "evt_end", "demand_impact")

    # High return flag
    df = df.withColumn(
        "high_return_flag",
        when(col("returns") > col("quantity_sold") * 0.10, True).otherwise(False)
    )

    df = df.withColumn("silver_processed_at", current_timestamp())

    # Write partitioned by year/month
    df.withColumn("year", expr("year(transaction_date)")) \
      .withColumn("month", expr("month(transaction_date)")) \
      .write \
      .mode("append") \
      .partitionBy("year", "month") \
      .parquet(SILVER_PATH)

    print(f"Batch {batch_id} written to Silver layer.")


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    drug_master, region_master, seasonal_events = load_master_data(spark)

    bronze_stream = (
        spark.readStream
        .format("parquet")
        .schema(spark.read.parquet(BRONZE_PATH).schema)
        .option("path", BRONZE_PATH)
        .load()
    )

    query = (
        bronze_stream.writeStream
        .foreachBatch(lambda df, bid: clean_and_enrich(df, bid, drug_master, region_master, seasonal_events))
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start()
    )

    print("Silver transformation stream started...")
    query.awaitTermination()


if __name__ == "__main__":
    run()
