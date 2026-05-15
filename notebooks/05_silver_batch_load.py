# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Batch Load

# COMMAND ----------

import logging
import uuid
from datetime import datetime
from typing import NamedTuple

from src.utils.logging import configure_logging
from src.utils.params import parse_month_range

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


configure_logging()
logger = logging.getLogger(__name__)

dbutils.widgets.text("start_month", "2024-01")
dbutils.widgets.text("end_month", "2024-06")

start_month = dbutils.widgets.get("start_month")
end_month   = dbutils.widgets.get("end_month")
parsed_month_range = parse_month_range(start_month, end_month)
year = parsed_month_range[0][0]
months_list = [month for _, month in parsed_month_range]
if any(parsed_year != year for parsed_year, _ in parsed_month_range):
    logger.warning(
        "Silver load assumes a single-year month range start_month=%s end_month=%s",
        start_month,
        end_month,
    )

spark.sql("USE CATALOG bronze")

BRONZE_TABLE = "default.yellow_trips_raw"
SILVER_CATALOG = "silver"
SILVER_SCHEMA = "default"
SILVER_CLEAN_TABLE = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.yellow_trips_clean"
SILVER_QUARANTINE_TABLE = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.yellow_quarantine"
SILVER_DQ_METRICS_TABLE = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.dq_metrics"
DEDUP_KEY = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "fare_amount",
]


class SilverWriteResult(NamedTuple):
    clean_count: int
    quarantine_count: int
    dedup_removed: int

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

bronze_df = spark.table(BRONZE_TABLE).filter(
    (F.col("year") == year) & F.col("month").isin(months_list)
)
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

bronze_df = spark.table(BRONZE_TABLE).filter(
    (F.col("year") == year) & F.col("month").isin(months_list)
)
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
# MAGIC ## Deduplication (3.6)

# COMMAND ----------

def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the latest clean row for each configured duplicate key."""
    if "_silver_ingestion_timestamp" not in df.columns:
        raise ValueError(
            "Missing _silver_ingestion_timestamp required for deduplication"
        )

    window_spec = Window.partitionBy(*DEDUP_KEY).orderBy(
        F.col("_silver_ingestion_timestamp").desc()
    )
    return (
        df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Quarantine Split + Write (3.5)

# COMMAND ----------

def write_silver(validated_df: DataFrame) -> SilverWriteResult:
    """Split validated rows and overwrite clean/quarantine Silver Delta tables."""
    logger.info("Starting silver write")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}")

    partitioned_df = validated_df.withColumn(
        "year", F.year(F.col("pickup_datetime"))
    ).withColumn("month", F.month(F.col("pickup_datetime")))

    validation_flag_columns = [
        "is_valid_fare_amount",
        "is_valid_trip_distance",
        "is_valid_passenger_count",
        "is_valid_datetime_order",
        "is_valid_trip_duration",
        "is_valid_total_amount",
        "is_valid_tip_amount",
        "is_valid_tolls_amount",
    ]

    clean_df = (
        partitioned_df.filter(F.col("is_valid") == True)
        .drop(*validation_flag_columns, "is_valid", "validation_failures")
    )
    before_count = clean_df.count()
    clean_df = deduplicate(clean_df)
    after_count = clean_df.count()
    logger.info("Dedup removed %s rows from clean", before_count - after_count)

    quarantine_df = (
        partitioned_df.filter(F.col("is_valid") == False)
        .drop(*validation_flag_columns, "is_valid")
    )

    hash_excluded_columns = {
        "_silver_ingestion_timestamp",
        "_source_file",
        "year",
        "month",
    }
    business_columns = sorted(
        c for c in clean_df.columns if c not in hash_excluded_columns
    )
    clean_df = clean_df.withColumn(
        "_row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                    for c in business_columns
                ],
            ),
            256,
        ),
    )

    if spark.catalog.tableExists(SILVER_CLEAN_TABLE):
        target = DeltaTable.forName(spark, SILVER_CLEAN_TABLE)
        merge_condition = " AND ".join(
            f"target.{col} = source.{col}" for col in DEDUP_KEY
        )
        (
            target.alias("target")
            .merge(clean_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll(
                condition=(
                    "source._row_hash != target._row_hash AND "
                    "source._silver_ingestion_timestamp > "
                    "target._silver_ingestion_timestamp"
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("MERGE completed on %s", SILVER_CLEAN_TABLE)
    else:
        (
            clean_df.write.format("delta")
            .partitionBy("year", "month")
            .saveAsTable(SILVER_CLEAN_TABLE)
        )
        logger.info("First-load created %s", SILVER_CLEAN_TABLE)

    quarantine_partition_filter = " OR ".join(
        f"(year = {year} AND month = {m})" for m in months_list
    )
    (
        quarantine_df.write.format("delta")
        .partitionBy("year", "month")
        .mode("overwrite")
        .option("replaceWhere", quarantine_partition_filter)
        .saveAsTable(SILVER_QUARANTINE_TABLE)
    )

    clean_count = spark.table(SILVER_CLEAN_TABLE).count()
    quarantine_count = spark.table(SILVER_QUARANTINE_TABLE).count()
    logger.info(
        "Silver write completed clean_count=%s quarantine_count=%s",
        clean_count,
        quarantine_count,
    )
    return SilverWriteResult(
        clean_count=clean_count,
        quarantine_count=quarantine_count,
        dedup_removed=before_count - after_count,
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## DQ Metrics (3.8)

# COMMAND ----------

def collect_rules_failed(validated_df: DataFrame) -> dict[str, int]:
    """Collect per-rule failure counts from the validated DataFrame as a Python dict."""
    rows = (
        validated_df.select(F.explode("validation_failures").alias("rule"))
        .groupBy("rule")
        .count()
        .collect()
    )
    return {row["rule"]: row["count"] for row in rows}


def write_dq_metrics(
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    rows_in: int,
    rows_clean: int,
    rows_quarantined: int,
    rows_deduped: int,
    rules_failed: dict[str, int],
    bronze_table: str,
) -> None:
    """Append one DQ metrics row for the current pipeline run."""
    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("started_at", TimestampType(), False),
            StructField("finished_at", TimestampType(), False),
            StructField("rows_in", LongType(), False),
            StructField("rows_clean", LongType(), False),
            StructField("rows_quarantined", LongType(), False),
            StructField("rows_deduped", LongType(), False),
            StructField("rules_failed", MapType(StringType(), LongType()), False),
            StructField("bronze_table", StringType(), False),
        ]
    )
    metrics_row = [
        (
            run_id,
            started_at,
            finished_at,
            rows_in,
            rows_clean,
            rows_quarantined,
            rows_deduped,
            rules_failed,
            bronze_table,
        )
    ]
    metrics_df = spark.createDataFrame(metrics_row, schema)
    (
        metrics_df.write.format("delta")
        .mode("append")
        .saveAsTable(SILVER_DQ_METRICS_TABLE)
    )
    logger.info("DQ metrics row appended for run_id=%s", run_id)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Pipeline

# COMMAND ----------

def silver_pipeline(bronze_table_name: str) -> tuple[int, int]:
    """Run the full Bronze -> Silver pipeline and write a DQ metrics row."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now()
    logger.info("Silver pipeline starting run_id=%s for %s", run_id, bronze_table_name)

    bronze_df = spark.table(bronze_table_name).filter(
        (F.col("year") == year) & F.col("month").isin(months_list)
    )
    rows_in = bronze_df.count()

    normalized_df = normalize_bronze(bronze_df)
    validated_df = validate_normalized(normalized_df)
    rules_failed = collect_rules_failed(validated_df)

    result = write_silver(validated_df)

    finished_at = datetime.now()
    write_dq_metrics(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        rows_in=rows_in,
        rows_clean=result.clean_count,
        rows_quarantined=result.quarantine_count,
        rows_deduped=result.dedup_removed,
        rules_failed=rules_failed,
        bronze_table=bronze_table_name,
    )

    logger.info(
        "Silver pipeline finished run_id=%s clean=%s quarantine=%s",
        run_id,
        result.clean_count,
        result.quarantine_count,
    )
    return (result.clean_count, result.quarantine_count)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write Silver Tables

# COMMAND ----------

clean_count, quarantine_count = silver_pipeline(BRONZE_TABLE)

print(f"Clean rows in {SILVER_CLEAN_TABLE}: {clean_count}")
print(f"Quarantine rows in {SILVER_QUARANTINE_TABLE}: {quarantine_count}")
