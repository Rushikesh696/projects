from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, lag, expr,
    when, lit, current_timestamp, month, year, quarter
)

SILVER_PATH = "data/silver/sales_enriched"
GOLD_PATH = "data/gold/ml_features"
CHECKPOINT_PATH = "data/checkpoints/gold"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("PharmaCast-Gold-FeatureEngineering")
        .getOrCreate()
    )


def build_features(df):
    # Monthly aggregation per drug per region
    monthly = df.groupBy(
        col("drug_id"), col("drug_name"), col("drug_category"), col("drug_type"),
        col("region_id"), col("region_name"), col("zone"), col("tier"),
        col("manufacturer"), col("cold_chain_required"), col("is_generic"),
        col("population_proxy"), col("num_hospitals"), col("num_clinics"),
        year("transaction_date").alias("year"),
        month("transaction_date").alias("month")
    ).agg(
        spark_sum("quantity_sold").alias("total_units_sold"),
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("returns").alias("total_returns"),
        avg("unit_price").alias("avg_unit_price"),
        count("transaction_id").alias("num_transactions"),
        spark_sum(when(col("channel") == "Hospital", col("quantity_sold")).otherwise(0)).alias("hospital_qty"),
        spark_sum(when(col("channel") == "Clinic", col("quantity_sold")).otherwise(0)).alias("clinic_qty"),
        spark_sum(when(col("channel") == "Retail", col("quantity_sold")).otherwise(0)).alias("retail_qty"),
        spark_sum(when(col("is_event_day") == True, col("quantity_sold")).otherwise(0)).alias("event_day_qty"),
        count(when(col("is_event_day") == True, 1)).alias("event_day_count"),
    )

    # Window for lag and rolling features
    w = Window.partitionBy("drug_id", "region_id").orderBy("year", "month")
    w3 = w.rowsBetween(-2, 0)
    w6 = w.rowsBetween(-5, 0)

    monthly = monthly \
        .withColumn("lag_1m", lag("total_units_sold", 1).over(w)) \
        .withColumn("lag_3m", lag("total_units_sold", 3).over(w)) \
        .withColumn("lag_6m", lag("total_units_sold", 6).over(w)) \
        .withColumn("lag_12m", lag("total_units_sold", 12).over(w)) \
        .withColumn("rolling_avg_3m", avg("total_units_sold").over(w3)) \
        .withColumn("rolling_avg_6m", avg("total_units_sold").over(w6)) \
        .withColumn("mom_growth_rate",
            when(col("lag_1m").isNotNull() & (col("lag_1m") != 0),
                 (col("total_units_sold") - col("lag_1m")) / col("lag_1m")
            ).otherwise(lit(0.0))
        )

    # Calendar features
    monthly = monthly \
        .withColumn("quarter", quarter(expr("make_date(year, month, 1)"))) \
        .withColumn("season",
            when(col("month").isin([12, 1, 2]), lit("Winter"))
            .when(col("month").isin([3, 4, 5]), lit("Spring"))
            .when(col("month").isin([6, 7, 8, 9]), lit("Monsoon"))
            .otherwise(lit("Autumn"))
        ) \
        .withColumn("is_flu_season", when(col("month").isin([11, 12, 1, 2]), lit(1)).otherwise(lit(0))) \
        .withColumn("is_monsoon", when(col("month").isin([6, 7, 8, 9]), lit(1)).otherwise(lit(0))) \
        .withColumn("is_festival_season", when(col("month").isin([10, 11]), lit(1)).otherwise(lit(0))) \
        .withColumn("is_summer", when(col("month").isin([4, 5, 6]), lit(1)).otherwise(lit(0)))

    monthly = monthly.withColumn("gold_processed_at", current_timestamp())

    return monthly


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.parquet(SILVER_PATH)
    features_df = build_features(silver_df)

    features_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(GOLD_PATH)

    print(f"Gold layer feature table written to {GOLD_PATH}")
    print(f"Total records: {features_df.count()}")
    print(f"Columns ({len(features_df.columns)}): {features_df.columns}")


if __name__ == "__main__":
    run()
