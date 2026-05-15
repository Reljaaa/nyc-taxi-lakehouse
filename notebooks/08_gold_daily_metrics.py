# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Daily Metrics By Zone

# COMMAND ----------

import logging

from src.utils.logging import configure_logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


configure_logging()
logger = logging.getLogger(__name__)

FCT_TRIPS_TABLE     = "gold.default.fct_trips"
DIM_ZONES_TABLE     = "gold.default.dim_zones"
DIM_DATE_TABLE      = "gold.default.dim_date"
DAILY_METRICS_TABLE = "gold.default.daily_metrics_by_zone"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build daily_metrics_by_zone

# COMMAND ----------

def build_daily_metrics() -> DataFrame:
    """Build daily pickup-zone metrics from the Gold fact table."""
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

    return (
        joined_df.groupBy(
            F.col("fct.pickup_date_sk"),
            F.col("fct.pickup_zone_sk"),
            F.col("dz.zone_name"),
            F.col("dz.borough"),
            F.col("dd.full_date"),
            F.col("dd.year"),
            F.col("dd.month"),
            F.col("dd.day_of_week"),
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("fct.total_amount").alias("total_revenue"),
            F.sum("fct.tip_amount").alias("total_tips"),
            F.avg("fct.fare_amount").alias("avg_fare"),
            F.avg("fct.trip_distance").alias("avg_distance"),
            F.avg("fct.trip_duration_seconds").alias("avg_duration_seconds"),
            F.avg("fct.tip_percentage").alias("avg_tip_percentage"),
        )
        .select(
            "pickup_date_sk",
            "pickup_zone_sk",
            "zone_name",
            "borough",
            "full_date",
            "year",
            "month",
            "day_of_week",
            "total_trips",
            "total_revenue",
            "total_tips",
            "avg_fare",
            "avg_distance",
            "avg_duration_seconds",
            "avg_tip_percentage",
        )
    )


def write_daily_metrics(df: DataFrame) -> None:
    """Overwrite the Gold daily pickup-zone metrics table."""
    logger.info("Writing Gold daily metrics table=%s", DAILY_METRICS_TABLE)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DAILY_METRICS_TABLE)
    )
    logger.info("Gold daily metrics write completed table=%s", DAILY_METRICS_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write and Validate daily_metrics_by_zone

# COMMAND ----------

daily_df = build_daily_metrics()

fct_row_count = spark.table(FCT_TRIPS_TABLE).count()
row_count = daily_df.count()
assert row_count > 0, "daily_metrics_by_zone is empty"
assert row_count <= fct_row_count, (
    f"daily_metrics_by_zone exceeds fct_trips row count: "
    f"daily={row_count}, fact={fct_row_count}"
)
assert row_count <= 366 * 266, f"Row count exceeds max possible grain: {row_count}"

write_daily_metrics(daily_df)

written_df = spark.table(DAILY_METRICS_TABLE)
written_count = written_df.count()
assert written_count == row_count, (
    f"Row count changed between pre-write and post-write: pre={row_count}, post={written_count}"
)

logger.info("daily_metrics_by_zone written rows=%s", written_count)

print(f"daily_metrics_by_zone rows: {written_count}")
written_df.printSchema()
written_df.show(5, truncate=False)
