# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Profiling

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


spark.sql("USE CATALOG bronze")

BRONZE_TABLE = "default.yellow_trips_raw"
LANDING_BASE_PATH = "abfss://landing@nyctaxilakehouse.dfs.core.windows.net"
ZONE_LOOKUP_PATH = f"{LANDING_BASE_PATH}/taxi_zone_lookup.csv"
ZONE_LOOKUP_TABLE = "default.taxi_zone_lookup"

bronze_df = spark.table(BRONZE_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## P1 — Null Rates

# COMMAND ----------

total_rows = bronze_df.count()
null_count_expressions = [
    F.count(F.when(F.col(f"`{column_name}`").isNull(), column_name)).alias(column_name)
    for column_name in bronze_df.columns
]
null_counts = bronze_df.select(null_count_expressions).first()
null_rate_rows = [
    (
        column_name,
        int(null_counts[column_name]),
        (float(null_counts[column_name]) / total_rows) * 100 if total_rows else 0.0,
    )
    for column_name in bronze_df.columns
]

null_rates_df = spark.createDataFrame(
    null_rate_rows,
    ["column_name", "null_count", "null_pct"],
).orderBy(F.desc("null_pct"), F.asc("column_name"))

display(null_rates_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## P2 — fare_amount Distribution

# COMMAND ----------

fare_amount_profile_df = spark.sql(
    f"""
    SELECT
        MIN(fare_amount) AS min_fare_amount,
        MAX(fare_amount) AS max_fare_amount,
        AVG(fare_amount) AS mean_fare_amount,
        percentile_approx(fare_amount, 0.50) AS p50_fare_amount,
        percentile_approx(fare_amount, 0.95) AS p95_fare_amount,
        percentile_approx(fare_amount, 0.99) AS p99_fare_amount,
        SUM(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END) AS fare_amount_lte_zero,
        SUM(CASE WHEN fare_amount > 500 THEN 1 ELSE 0 END) AS fare_amount_gt_500
    FROM {BRONZE_TABLE}
    """
)

display(fare_amount_profile_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## P3 — trip_distance Distribution

# COMMAND ----------

trip_distance_profile_df = spark.sql(
    f"""
    SELECT
        MIN(trip_distance) AS min_trip_distance,
        MAX(trip_distance) AS max_trip_distance,
        AVG(trip_distance) AS mean_trip_distance,
        percentile_approx(trip_distance, 0.50) AS p50_trip_distance,
        percentile_approx(trip_distance, 0.95) AS p95_trip_distance,
        percentile_approx(trip_distance, 0.99) AS p99_trip_distance,
        SUM(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END) AS trip_distance_lte_zero,
        SUM(CASE WHEN trip_distance > 200 THEN 1 ELSE 0 END) AS trip_distance_gt_200
    FROM {BRONZE_TABLE}
    """
)

display(trip_distance_profile_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## P4 — passenger_count Distribution

# COMMAND ----------

passenger_count_distribution_df = spark.sql(
    f"""
    SELECT
        passenger_count,
        COUNT(*) AS row_count
    FROM {BRONZE_TABLE}
    GROUP BY passenger_count
    ORDER BY passenger_count
    """
)

display(passenger_count_distribution_df)

passenger_count_outlier_df = spark.sql(
    f"""
    SELECT
        SUM(CASE WHEN passenger_count = 0 THEN 1 ELSE 0 END) AS passenger_count_zero,
        SUM(CASE WHEN passenger_count > 6 THEN 1 ELSE 0 END) AS passenger_count_gt_6
    FROM {BRONZE_TABLE}
    """
)

display(passenger_count_outlier_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## P5 — Datetime Ordering

# COMMAND ----------

datetime_ordering_df = spark.sql(
    f"""
    SELECT
        COUNT(*) AS dropoff_before_or_equal_pickup_count
    FROM {BRONZE_TABLE}
    WHERE tpep_dropoff_datetime <= tpep_pickup_datetime
    """
)

display(datetime_ordering_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## P6 — Trip Duration

# COMMAND ----------

trip_duration_profile_df = spark.sql(
    f"""
    WITH trip_duration AS (
        SELECT
            (
                unix_timestamp(tpep_dropoff_datetime)
                - unix_timestamp(tpep_pickup_datetime)
            ) / 60 AS duration_minutes
        FROM {BRONZE_TABLE}
    )
    SELECT
        MIN(duration_minutes) AS min_duration_minutes,
        MAX(duration_minutes) AS max_duration_minutes,
        AVG(duration_minutes) AS mean_duration_minutes,
        percentile_approx(duration_minutes, 0.50) AS p50_duration_minutes,
        percentile_approx(duration_minutes, 0.95) AS p95_duration_minutes,
        percentile_approx(duration_minutes, 0.99) AS p99_duration_minutes,
        SUM(CASE WHEN duration_minutes < 0 THEN 1 ELSE 0 END) AS duration_lt_zero,
        SUM(CASE WHEN duration_minutes > 1440 THEN 1 ELSE 0 END) AS duration_gt_1440
    FROM trip_duration
    """
)

display(trip_duration_profile_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## P7 — Zone ID Validity

# COMMAND ----------

zone_lookup_df = None
zone_lookup_source = None

try:
    zone_lookup_df = spark.read.option("header", True).csv(ZONE_LOOKUP_PATH)
    zone_lookup_source = ZONE_LOOKUP_PATH
except Exception:
    try:
        zone_lookup_df = spark.table(ZONE_LOOKUP_TABLE)
        zone_lookup_source = ZONE_LOOKUP_TABLE
    except AnalysisException:
        # Zone lookup not found at the expected landing path or bronze table; skip P7.
        display(
            spark.createDataFrame(
                [
                    (
                        "P7 skipped",
                        f"Zone lookup not found at {ZONE_LOOKUP_PATH} or {ZONE_LOOKUP_TABLE}",
                    )
                ],
                ["check", "message"],
            )
        )

if zone_lookup_df is not None:
    zone_ids_df = zone_lookup_df.select(
        F.col("LocationID").cast("int").alias("location_id")
    ).distinct()

    pickup_invalid_zone_df = (
        bronze_df.select(F.col("PULocationID").cast("int").alias("location_id"))
        .distinct()
        .join(zone_ids_df, on="location_id", how="left_anti")
        .withColumn("zone_id_type", F.lit("PULocationID"))
    )
    dropoff_invalid_zone_df = (
        bronze_df.select(F.col("DOLocationID").cast("int").alias("location_id"))
        .distinct()
        .join(zone_ids_df, on="location_id", how="left_anti")
        .withColumn("zone_id_type", F.lit("DOLocationID"))
    )
    invalid_zone_ids_df = pickup_invalid_zone_df.unionByName(dropoff_invalid_zone_df)

    display(
        spark.createDataFrame(
            [(zone_lookup_source,)],
            ["zone_lookup_source"],
        )
    )
    display(invalid_zone_ids_df.orderBy("zone_id_type", "location_id"))
    display(
        invalid_zone_ids_df.groupBy("zone_id_type")
        .count()
        .orderBy("zone_id_type")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## P8 — Duplicate Candidate Analysis

# COMMAND ----------

candidate_a_duplicate_df = spark.sql(
    f"""
    SELECT
        'candidate_a_simple' AS candidate_key,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT struct(VendorID, tpep_pickup_datetime, PULocationID))
            AS distinct_key_combinations,
        COUNT(*)
        - COUNT(DISTINCT struct(VendorID, tpep_pickup_datetime, PULocationID))
            AS duplicate_row_count
    FROM {BRONZE_TABLE}
    """
)

candidate_b_duplicate_df = spark.sql(
    f"""
    SELECT
        'candidate_b_strict' AS candidate_key,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT struct(
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            fare_amount
        )) AS distinct_key_combinations,
        COUNT(*)
        - COUNT(DISTINCT struct(
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            fare_amount
        )) AS duplicate_row_count
    FROM {BRONZE_TABLE}
    """
)

display(candidate_a_duplicate_df.unionByName(candidate_b_duplicate_df))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary of Findings
# MAGIC - [ ] Fill in after reviewing query outputs on Databricks
