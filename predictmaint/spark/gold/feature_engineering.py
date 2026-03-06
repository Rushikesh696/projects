from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, avg, stddev, min as spark_min, max as spark_max,
    lag, expr, current_timestamp, lit, when, count
)

SILVER_PATH = "data/silver/sensor_enriched"
GOLD_PATH = "data/gold/ml_features"

SENSORS = ["temperature", "vibration", "pressure", "rpm", "voltage"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("PredictMaint-Gold-FeatureEngineering")
        .getOrCreate()
    )


def build_features(df):
    # Window specs
    w_machine = Window.partitionBy("machine_id").orderBy("timestamp")
    w_5min  = Window.partitionBy("machine_id").orderBy(expr("unix_timestamp(timestamp)")).rangeBetween(-300,  0)
    w_15min = Window.partitionBy("machine_id").orderBy(expr("unix_timestamp(timestamp)")).rangeBetween(-900,  0)
    w_30min = Window.partitionBy("machine_id").orderBy(expr("unix_timestamp(timestamp)")).rangeBetween(-1800, 0)

    for sensor in SENSORS:
        # Lag features
        df = df \
            .withColumn(f"{sensor}_lag1",  lag(col(sensor), 1).over(w_machine)) \
            .withColumn(f"{sensor}_lag5",  lag(col(sensor), 5).over(w_machine)) \
            .withColumn(f"{sensor}_lag10", lag(col(sensor), 10).over(w_machine))

        # Rate of change
        df = df.withColumn(
            f"{sensor}_roc",
            when(col(f"{sensor}_lag1").isNotNull() & (col(f"{sensor}_lag1") != 0),
                 (col(sensor) - col(f"{sensor}_lag1")) / col(f"{sensor}_lag1")
            ).otherwise(lit(0.0))
        )

        # Rolling stats - 5 min
        df = df \
            .withColumn(f"{sensor}_mean_5m",   avg(col(sensor)).over(w_5min)) \
            .withColumn(f"{sensor}_std_5m",    stddev(col(sensor)).over(w_5min)) \
            .withColumn(f"{sensor}_min_5m",    spark_min(col(sensor)).over(w_5min)) \
            .withColumn(f"{sensor}_max_5m",    spark_max(col(sensor)).over(w_5min))

        # Rolling stats - 15 min
        df = df \
            .withColumn(f"{sensor}_mean_15m",  avg(col(sensor)).over(w_15min)) \
            .withColumn(f"{sensor}_std_15m",   stddev(col(sensor)).over(w_15min))

        # Rolling stats - 30 min
        df = df \
            .withColumn(f"{sensor}_mean_30m",  avg(col(sensor)).over(w_30min)) \
            .withColumn(f"{sensor}_std_30m",   stddev(col(sensor)).over(w_30min))

    # Anomaly count in last 5 min window
    df = df.withColumn("anomaly_count_5m", count(when(col("is_anomaly"), 1)).over(w_5min))

    # Calendar features
    df = df \
        .withColumn("hour",    expr("hour(timestamp)")) \
        .withColumn("day_of_week", expr("dayofweek(timestamp)")) \
        .withColumn("is_night_shift", when(col("hour").between(22, 6), lit(1)).otherwise(lit(0)))

    df = df.withColumn("gold_processed_at", current_timestamp())

    return df


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.parquet(SILVER_PATH)
    features_df = build_features(silver_df)

    features_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(GOLD_PATH)

    print(f"Gold layer written to {GOLD_PATH}")
    print(f"Total records: {features_df.count()}")
    print(f"Total features: {len(features_df.columns)}")


if __name__ == "__main__":
    run()
