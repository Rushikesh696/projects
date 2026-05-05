import os                                                                                                                                                                           
import sys      
import logging
from pathlib import Path
                                                                                                                                                                                    
from pyspark.sql import SparkSession
from pyspark.sql import functions as F                                                                                                                                              
from pyspark.sql.window import Window
from dotenv import load_dotenv
import yaml

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "configs" / "config.yaml"
LOG_PATH = BASE_DIR / "logs" / "silver_to_gold.log"                                                                                                                                 
LOG_PATH.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,                                                                                                                                                             
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[                                                                                                                                                                      
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH),                                                                                                                                              
    ]           
)
log = logging.getLogger(__name__)

def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)
    

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("SerumQA-SilverToGold")
        .config("spark.jars", str(BASE_DIR / "drivers" / "postgresql-42.7.3.jar"))
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def read_silver(spark: SparkSession):                                                                                                                                               
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
    df = spark.read.jdbc(jdbc_url, "silver.qa_events_clean", properties=properties)                                                                                                 
    log.info(f"Read {df.count():,} rows from silver.qa_events_clean")
    return df

def engineer_features(df):
    log.info("Engineering features...")

    # Convert timestamp to unix seconds for window calculations
    df = df.withColumn("ts", F.unix_timestamp("timestamp"))

    # 7-day window (in seconds) ordered by timestamp
    days_7 = 7 * 24 * 60 * 60
    days_30 = 30 * 24 * 60 *60

    w7 = Window.partitionBy("system").orderBy("ts").rangeBetween(-days_7, 0)
    w30 = Window.partitionBy("system").orderBy("ts").rangeBetween(-days_30, 0)

    # Rolling feature Engineering
    df = df.withColumn("deviation_count_7d",
                       F.sum(F.when(F.col("event_type") == "deviation", 1).otherwise(0)).over(w7))
    
    df = df.withColumn("deviation_count_30d",
                       F.sum(F.when(F.col("event_type") == "deviation", 1).otherwise(0)).over(w30))
    
    df = df.withColumn("complaint_rate_7d",
                       F.sum(F.when(F.col("event_type") == "complaint", 1).otherwise(0)).over(w7))
    
    df = df.withColumn("capa_overdue_ratio_30d",
                       F.sum(F.when((F.col("event_type") == "capa") & (F.col("status") == "Overdue"), 1).otherwise(0)).over(w30))
    
    df = df.withColumn("oos_count_7d",
                       F.sum(F.when(F.col("event_type") == "oos", 1).otherwise(0)).over(w7))
    
    df = df.withColumn("critical_ratio_7d",
                        F.sum(F.when(F.col("severity") == "Critical", 1).otherwise(0)).over(w7) / F.count("*").over(w7))
    
    df = df.withColumn("major_ratio_7d",
                       F.sum(F.when(F.col("severity") == "Major", 1).otherwise(0)).over(w7) / F.count("*").over(w7))
    
    df = df.withColumn("unusual_event_count_7d",
                        F.sum(F.when(F.col("event_type").isin("critical_alarm", "media_fill"), 1).otherwise(0)).over(w7))
    
    df = df.withColumn("material_complaint_count_7d",
                       F.sum(F.when((F.col("event_type") == "complaint") & (F.col("root_cause") == "Material"), 1).otherwise(0)).over(w7))
    
    df = df.withColumn("quality_risk_open_7d",
                       F.sum(F.when((F.col("status") == "Open") | (F.col("status") == "Under Investigation"), 1).otherwise(0)).over(w7))
    
    log.info("Feature engineering complete.")

    return df

def write_to_gold(df) -> None:
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

    log.info("Writing to gold.qa_features...")

    df = df.withColumnRenamed("id", "silver_id").select(                                                                                                                                
        "silver_id", "timestamp", "event_type", "product", "system",                                                                                                                    
        "severity", "is_anomaly", "is_covid_period",                                                                                                                                    
        "deviation_count_7d", "deviation_count_30d", "complaint_rate_7d",                                                                                                               
        "capa_overdue_ratio_30d", "oos_count_7d", "critical_ratio_7d",                                                                                                                  
        "major_ratio_7d", "unusual_event_count_7d", "material_complaint_count_7d",
        "quality_risk_open_7d", "hour", "day_of_week", "month", "is_weekend"                                                                                                            
    )

    (
        df.write
        .option("truncate", "true")
        .jdbc(jdbc_url, "gold.qa_features", mode="overwrite", properties=properties)
    )

    log.info(f"Write complete. {df.count():,} rows written to gold.qa_features")

def main():
    config = load_config()
    spark = get_spark()

    try:
        df = read_silver(spark)
        df = engineer_features(df)
        write_to_gold(df)
        log.info("Silver → Gold pipeline complete.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


    
