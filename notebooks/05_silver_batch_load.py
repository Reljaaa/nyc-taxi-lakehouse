# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Batch Load

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


spark.sql("USE CATALOG bronze")

BRONZE_TABLE = "default.yellow_trips_raw"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Normalization — Step 1: Type Casting + Column Normalization

# COMMAND ----------

def normalize_bronze(df: DataFrame) -> DataFrame:
    """Normalize bronze yellow taxi rows for the first Silver pipeline step."""
    normalized_df = df.select(
        F.col("VendorID").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("passenger_count").cast("integer").alias("passenger_count"),
        F.col("trip_distance").alias("trip_distance"),
        F.col("RatecodeID").cast("integer").alias("rate_code_id"),
        F.col("store_and_fwd_flag").alias("store_and_fwd_flag"),
        F.col("PULocationID").alias("pickup_location_id"),
        F.col("DOLocationID").alias("dropoff_location_id"),
        F.col("payment_type").cast("integer").alias("payment_type"),
        F.col("fare_amount").alias("fare_amount"),
        F.col("extra").alias("extra"),
        F.col("mta_tax").alias("mta_tax"),
        F.col("tip_amount").alias("tip_amount"),
        F.col("tolls_amount").alias("tolls_amount"),
        F.col("improvement_surcharge").alias("improvement_surcharge"),
        F.col("total_amount").alias("total_amount"),
        F.col("congestion_surcharge").alias("congestion_surcharge"),
        F.col("Airport_fee").alias("airport_fee"),
        F.col("_source_file").alias("_source_file"),
    )

    return normalized_df.withColumn(
        "trip_duration_minutes",
        (
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            )
            / F.lit(60.0)
        ).cast("double"),
    ).withColumn("_silver_ingestion_timestamp", F.current_timestamp())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Demonstration

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
normalized_df = normalize_bronze(bronze_df)

normalized_df.printSchema()
display(normalized_df.limit(5))

print(
    f"Bronze columns: {len(bronze_df.columns)}, "
    f"Silver columns: {len(normalized_df.columns)}"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Validation Logic (3.4)
# MAGIC ## Step 3: Quarantine Split + Write (3.5)
