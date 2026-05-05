import os
import sys
import logging
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import yaml

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)

load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "configs" / "config.yaml"
CSV_PATH = BASE_DIR / "data" / "bronze" / "serum_qa_realtime_raw.csv"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("SerumQA-producer")
        .config("spark.jars", str(BASE_DIR / "drivers" / "postgresql-42.7.3.jar"))
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


QA_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("product", StringType(), True),
        StructField("system", StringType(), True),
        StructField("root_cause", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("resolution_days", FloatType(), True),
        StructField("closed_timestamp", TimestampType(), True),
        StructField("is_anomaly", IntegerType(), True),
        StructField("is_covid_period", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("is_weekend", IntegerType(), True),
    ]
)


def load_csv(spark: SparkSession):
    log.info(f"Reading csv: {CSV_PATH}")
    df = (
        spark.read.option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(QA_SCHEMA)
        .csv(str(CSV_PATH))
    )
    log.info(f"Loaded {df.count():,} rows")
    return df


def write_to_postgres(df, config: dict) -> None:
    jdbc_url = (
        f"jdbc:postgresql://"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5433')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

    properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }

    batch_size = config["pipeline"]["batch_size"]

    log.info(f"Writing to source.qa_events - batch_size ={batch_size:,}")
    t0 = time.time()

    (
        df.write.option("batchsize", batch_size)
        .option("truncate", "true")
        .jdbc(jdbc_url, "source.qa_events", mode="overwrite", properties=properties)
    )

    elapsed = time.time() - t0
    log.info(f"Write complete in {elapsed:.1f}s")


def main():
    config = load_config()
    spark = get_spark()

    try:
        df = load_csv(spark)
        write_to_postgres(df, config)
        log.info(
            "Done. Debezium should now be streaming CDC events to kafka topic: serum.source.qa_events"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
