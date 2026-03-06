from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, TimestampType
)

KAFKA_BROKER = "localhost:9092"
TOPIC = "pharma.sales.transactions"
BRONZE_PATH = "data/bronze/sales_transactions"
CHECKPOINT_PATH = "data/checkpoints/bronze"

SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("drug_id", StringType()),
    StructField("drug_name", StringType()),
    StructField("drug_category", StringType()),
    StructField("drug_type", StringType()),
    StructField("region_id", StringType()),
    StructField("region_name", StringType()),
    StructField("zone", StringType()),
    StructField("tier", StringType()),
    StructField("channel", StringType()),
    StructField("quantity_sold", IntegerType()),
    StructField("unit_price", FloatType()),
    StructField("returns", IntegerType()),
    StructField("revenue", FloatType()),
    StructField("cold_chain_required", BooleanType()),
    StructField("is_generic", BooleanType()),
    StructField("manufacturer", StringType()),
    StructField("ingestion_source", StringType()),
    StructField("ingestion_timestamp", StringType()),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("PharmaCast-Bronze-Ingestion")
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
        .select(from_json(col("value").cast("string"), SCHEMA).alias("data"), col("timestamp").alias("kafka_timestamp"))
        .select("data.*", "kafka_timestamp")
        .withColumn("bronze_ingested_at", current_timestamp())
        .withColumn("source_system", lit("kafka"))
        .withColumn("ingestion_date", col("timestamp").cast("date"))
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

    print("Bronze ingestion stream started...")
    query.awaitTermination()


if __name__ == "__main__":
    run()
