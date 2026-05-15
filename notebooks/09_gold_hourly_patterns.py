# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Hourly Demand Patterns

# COMMAND ----------

import logging

from src.utils.logging import configure_logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


configure_logging()
logger = logging.getLogger(__name__)

FCT_TRIPS_TABLE       = "gold.default.fct_trips"
DIM_ZONES_TABLE       = "gold.default.dim_zones"
DIM_DATE_TABLE        = "gold.default.dim_date"
HOURLY_PATTERNS_TABLE = "gold.default.hourly_demand_patterns"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build hourly_demand_patterns

# COMMAND ----------

def build_hourly_patterns() -> DataFrame:
    """Build hourly pickup-zone demand patterns from the Gold fact table."""
    fct = spark.table(FCT_TRIPS_TABLE).alias("fct")
    dz = spark.table(DIM_ZONES_TABLE).alias("dz")
    dd = spark.table(DIM_DATE_TABLE).alias("dd")

    joined_df = (
        fct.join(
            dz,
            F.col("fct.pickup_zone_sk") == F.col("dz.zone_sk"),
            "inner",
        )
        .join(
            dd,
            F.col("fct.pickup_date_sk") == F.col("dd.date_sk"),
            "inner",
        )
    )

    step1_df = (
        joined_df.groupBy(
            F.col("fct.pickup_date_sk"),
            F.col("dd.day_of_week"),
            F.col("fct.pickup_hour"),
            F.col("fct.pickup_zone_sk"),
            F.col("dz.zone_name"),
            F.col("dz.borough"),
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fct.fare_amount").alias("avg_fare_per_slot"),
        )
    )

    fct_row_count = fct.count()
    step1_row_count = step1_df.count()
    assert step1_row_count <= fct_row_count, (
        f"hourly_demand_patterns step 1 exceeds fct_trips row count: "
        f"step1={step1_row_count}, fact={fct_row_count}"
    )

    return (
        step1_df.groupBy(
            "day_of_week",
            "pickup_hour",
            "pickup_zone_sk",
            "zone_name",
            "borough",
        )
        .agg(
            F.avg("trip_count").alias("avg_trips"),
            F.avg("avg_fare_per_slot").alias("avg_fare"),
        )
        .select(
            "day_of_week",
            "pickup_hour",
            "pickup_zone_sk",
            "zone_name",
            "borough",
            "avg_trips",
            "avg_fare",
        )
    )


def write_hourly_patterns(df: DataFrame) -> None:
    """Overwrite the Gold hourly demand patterns table."""
    logger.info("Writing Gold hourly patterns table=%s", HOURLY_PATTERNS_TABLE)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(HOURLY_PATTERNS_TABLE)
    )
    logger.info("Gold hourly patterns write completed table=%s", HOURLY_PATTERNS_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write and Validate hourly_demand_patterns

# COMMAND ----------

hourly_df = build_hourly_patterns()

row_count = hourly_df.count()
assert row_count > 0, "hourly_demand_patterns is empty"
assert row_count <= 7 * 24 * 266, f"Row count exceeds max possible grain: {row_count}"

write_hourly_patterns(hourly_df)

written_df = spark.table(HOURLY_PATTERNS_TABLE)
written_count = written_df.count()
assert written_count == row_count, (
    f"Row count changed between pre-write and post-write: pre={row_count}, post={written_count}"
)

logger.info("hourly_demand_patterns written rows=%s", written_count)

print(f"hourly_demand_patterns rows: {written_count}")
written_df.printSchema()
written_df.show(5, truncate=False)
