from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, when, current_timestamp,
    trim, broadcast, expr
)

BRONZE_PATH = "data/bronze/sensor_readings"
SILVER_PATH = "data/silver/sensor_enriched"
CHECKPOINT_PATH = "data/checkpoints/silver"
RAW_PATH = "data/raw"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("PredictMaint-Silver-Transform")
        .getOrCreate()
    )


def load_machine_master(spark):
    return spark.read.csv(f"{RAW_PATH}/machine_master.csv", header=True, inferSchema=True)


def clean_and_enrich(batch_df, batch_id, machine_master):
    df = batch_df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("reading_date", to_date(col("timestamp")))

    # Drop nulls on critical fields
    df = df.dropna(subset=["reading_id", "machine_id", "temperature", "vibration", "pressure"])

    # Deduplication
    df = df.dropDuplicates(["reading_id"])

    # Trim strings
    for c in ["machine_id", "machine_type", "location"]:
        df = df.withColumn(c, trim(col(c)))

    # Join machine master for thresholds
    master_cols = machine_master.select(
        "machine_id", "age_years", "criticality", "manufacturer",
        "temp_critical", "vibration_critical", "pressure_critical",
        "rpm_critical", "voltage_critical"
    )
    df = df.join(broadcast(master_cols), on="machine_id", how="left")

    # Anomaly flags
    df = df \
        .withColumn("temp_anomaly",      when(col("temperature") > col("temp_critical"),      True).otherwise(False)) \
        .withColumn("vibration_anomaly", when(col("vibration")   > col("vibration_critical"), True).otherwise(False)) \
        .withColumn("pressure_anomaly",  when(col("pressure")    > col("pressure_critical"),  True).otherwise(False)) \
        .withColumn("rpm_anomaly",       when(col("rpm")         > col("rpm_critical"),        True).otherwise(False)) \
        .withColumn("sensor_out_of_range",
            when(
                col("temp_anomaly") | col("vibration_anomaly") |
                col("pressure_anomaly") | col("rpm_anomaly"), True
            ).otherwise(False)
        ) \
        .withColumn("is_anomaly",
            when(col("sensor_out_of_range") | col("is_degrading"), True).otherwise(False)
        )

    df = df.withColumn("silver_processed_at", current_timestamp())

    df.withColumn("year",  expr("year(reading_date)")) \
      .withColumn("month", expr("month(reading_date)")) \
      .write \
      .mode("append") \
      .partitionBy("year", "month") \
      .parquet(SILVER_PATH)

    print(f"Batch {batch_id} written to Silver layer. Records: {df.count()}")


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    machine_master = load_machine_master(spark)

    bronze_stream = (
        spark.readStream
        .format("parquet")
        .schema(spark.read.parquet(BRONZE_PATH).schema)
        .option("path", BRONZE_PATH)
        .load()
    )

    query = (
        bronze_stream.writeStream
        .foreachBatch(lambda df, bid: clean_and_enrich(df, bid, machine_master))
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start()
    )

    print("Silver transformation stream started...")
    query.awaitTermination()


if __name__ == "__main__":
    run()
