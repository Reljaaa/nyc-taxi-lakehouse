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

# COMMAND ----------

def validate_normalized(df: DataFrame) -> DataFrame:
    """Add per-row Silver validation flags and failure reason codes."""
    return (
        df.withColumn(
            "is_valid_fare_amount",
            F.col("fare_amount").isNull()
            | ((F.col("fare_amount") > 0) & (F.col("fare_amount") <= 500)),
        )
        .withColumn(
            "is_valid_trip_distance",
            F.col("trip_distance").isNull()
            | ((F.col("trip_distance") > 0) & (F.col("trip_distance") <= 200)),
        )
        .withColumn(
            "is_valid_passenger_count",
            F.col("passenger_count").isNull()
            | ((F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6)),
        )
        .withColumn(
            "is_valid_datetime_order",
            F.col("pickup_datetime").isNull()
            | F.col("dropoff_datetime").isNull()
            | (F.col("dropoff_datetime") > F.col("pickup_datetime")),
        )
        .withColumn(
            "is_valid_trip_duration",
            F.col("trip_duration_minutes").isNull()
            | (F.col("trip_duration_minutes") <= 180),
        )
        .withColumn(
            "is_valid_total_amount",
            F.col("total_amount").isNull() | (F.col("total_amount") > 0),
        )
        .withColumn(
            "is_valid_tip_amount",
            F.col("tip_amount").isNull() | (F.col("tip_amount") >= 0),
        )
        .withColumn(
            "is_valid_tolls_amount",
            F.col("tolls_amount").isNull() | (F.col("tolls_amount") >= 0),
        )
        .withColumn(
            "validation_failures",
            F.array_compact(
                F.array(
                    F.when(
                        ~F.col("is_valid_fare_amount"),
                        F.lit("invalid_fare_amount"),
                    ),
                    F.when(
                        ~F.col("is_valid_trip_distance"),
                        F.lit("invalid_trip_distance"),
                    ),
                    F.when(
                        ~F.col("is_valid_passenger_count"),
                        F.lit("invalid_passenger_count"),
                    ),
                    F.when(
                        ~F.col("is_valid_datetime_order"),
                        F.lit("invalid_datetime_order"),
                    ),
                    F.when(
                        ~F.col("is_valid_trip_duration"),
                        F.lit("invalid_trip_duration"),
                    ),
                    F.when(
                        ~F.col("is_valid_total_amount"),
                        F.lit("invalid_total_amount"),
                    ),
                    F.when(
                        ~F.col("is_valid_tip_amount"),
                        F.lit("invalid_tip_amount"),
                    ),
                    F.when(
                        ~F.col("is_valid_tolls_amount"),
                        F.lit("invalid_tolls_amount"),
                    ),
                )
            ),
        )
        .withColumn("is_valid", F.size(F.col("validation_failures")) == 0)
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation Results

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
normalized_df = normalize_bronze(bronze_df)
validated_df = validate_normalized(normalized_df)

total_row_count = validated_df.count()
valid_row_count = validated_df.filter(F.col("is_valid") == True).count()
invalid_row_count = validated_df.filter(F.col("is_valid") == False).count()

print(f"Total rows: {total_row_count}")
print(f"Valid rows: {valid_row_count}")
print(f"Invalid rows: {invalid_row_count}")

validation_failure_counts_df = (
    validated_df.select(F.explode("validation_failures").alias("validation_failure"))
    .groupBy("validation_failure")
    .count()
    .orderBy(F.desc("count"))
)

display(validation_failure_counts_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Quarantine Split + Write (3.5)
