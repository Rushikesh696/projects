import logging
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "configs" / "config.yaml"
LOG_PATH = BASE_DIR / "logs" / "bronze_to_silver.log"
LOG_PATH.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH),
    ],
)
log = logging.getLogger(__name__)


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("SerumQA-BronzeToSilver")
        .config("spark.jars", str(BASE_DIR / "drivers" / "postgresql-42.7.3.jar"))
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession) -> object:
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

    df = spark.read.jdbc(jdbc_url, "bronze.qa_events", properties=properties)
    log.info(f"Read {df.count():,} rows from bronze.qa_events")
    return df


def clean_data(df):
    log.info("Starting data cleaning...")

    # 1. Remove duplicates
    before = df.count()
    df = df.dropDuplicates(["event_id"])
    after = df.count()
    log.info(f"Removed {before - after:,} duplicate rows")

    # 2. Impute missing values
    df = df.fillna(
        {
            "product": "Unknown",
            "system": "Unknown",
            "root_cause": "Unknown",
        }
    )

    # 3. Normalize severity — fix mislabeled values
    valid_severities = ["Critical", "Major", "Minor"]
    df = df.withColumn(
        "severity",
        F.when(F.col("severity").isin(valid_severities), F.col("severity")).otherwise("Minor"),
    )

    log.info("Data cleaning complete.")
    return df


def write_to_silver(df, spark: SparkSession) -> None:
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

    log.info("Writing to silver.qa_events_clean...")

    df = df.withColumnRenamed("id", "bronze_id").select(
        "bronze_id",
        "timestamp",
        "event_type",
        "event_id",
        "product",
        "system",
        "root_cause",
        "severity",
        "batch_id",
        "status",
        "resolution_days",
        "closed_timestamp",
        "is_anomaly",
        "is_covid_period",
        "hour",
        "day_of_week",
        "month",
        "year",
        "is_weekend",
    )

    (
        df.write.option("truncate", "true").jdbc(
            jdbc_url, "silver.qa_events_clean", mode="overwrite", properties=properties
        )
    )
    log.info(f"Write complete. {df.count():,} rows written to silver.qa_events_clean")


def main():
    spark = get_spark()

    try:
        df = read_bronze(spark)
        df = clean_data(df)
        write_to_silver(df, spark)
        log.info("Bronze → Silver pipeline complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
