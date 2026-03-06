from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, BooleanType, DoubleType
)

KAFKA_BROKER = "localhost:9092"
TOPIC = "iot.sensor.readings"
BRONZE_PATH = "data/bronze/sensor_readings"
CHECKPOINT_PATH = "data/checkpoints/bronze"

SCHEMA = StructType([
    StructField("reading_id", StringType()),
    StructField("machine_id", StringType()),
    StructField("machine_name", StringType()),
    StructField("machine_type", StringType()),
    StructField("location", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("rpm", DoubleType()),
    StructField("voltage", DoubleType()),
    StructField("degradation_factor", DoubleType()),
    StructField("is_degrading", BooleanType()),
    StructField("will_fail_in_4hrs", BooleanType()),
    StructField("ingestion_source", StringType()),
    StructField("ingestion_timestamp", StringType()),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("PredictMaint-Bronze-Ingestion")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3")
        .getOrCreate()
    )


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("bronze_ingested_at", current_timestamp())
        .withColumn("source_system", lit("iot_kafka"))
        .withColumn("ingestion_date", to_date(col("timestamp")))
    )

    query = (
        parsed.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingestion_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("Bronze IoT ingestion stream started...")
    query.awaitTermination()


if __name__ == "__main__":
    run()
