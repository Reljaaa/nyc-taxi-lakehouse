# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Validation

# COMMAND ----------

import logging

from src.utils.logging import configure_logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


configure_logging()
logger = logging.getLogger(__name__)

SILVER_CLEAN_TABLE  = "silver.default.yellow_trips_clean"
FCT_TRIPS_TABLE     = "gold.default.fct_trips"
DIM_ZONES_TABLE     = "gold.default.dim_zones"
DIM_DATE_TABLE      = "gold.default.dim_date"

GOLD_DATE_MIN = 20240101
GOLD_DATE_MAX = 20241231

# COMMAND ----------
# MAGIC %md
# MAGIC ## Row Count Reconciliation

# COMMAND ----------

def reconcile_row_counts() -> dict:
    """Compare silver and gold row counts; return counts and the filtered delta."""
    silver_count = spark.table(SILVER_CLEAN_TABLE).count()
    gold_count = spark.table(FCT_TRIPS_TABLE).count()
    date_filtered = silver_count - gold_count

    assert date_filtered >= 0, (
        f"Gold has more rows than Silver: silver={silver_count}, gold={gold_count}"
    )
    assert date_filtered <= 100, (
        f"Too many rows filtered by Gold date boundary: filtered={date_filtered}"
    )

    results = {
        "silver_count": silver_count,
        "gold_count": gold_count,
        "date_filtered": date_filtered,
    }
    logger.info("Gold row count reconciliation results=%s", results)
    return results

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregate Sum Check

# COMMAND ----------

def reconcile_sums() -> dict:
    """Compare fare/tip/total sums between silver (date-filtered) and gold."""
    silver_df = spark.table(SILVER_CLEAN_TABLE).filter(
        F.col("pickup_datetime").between("2024-01-01", "2024-12-31 23:59:59")
    )
    gold_df = spark.table(FCT_TRIPS_TABLE)

    silver_sums = silver_df.agg(
        F.sum("fare_amount").alias("silver_fare_sum"),
        F.sum("tip_amount").alias("silver_tip_sum"),
        F.sum("total_amount").alias("silver_total_sum"),
    ).first()
    gold_sums = gold_df.agg(
        F.sum("fare_amount").alias("gold_fare_sum"),
        F.sum("tip_amount").alias("gold_tip_sum"),
        F.sum("total_amount").alias("gold_total_sum"),
    ).first()

    silver_fare_sum = silver_sums["silver_fare_sum"]
    silver_tip_sum = silver_sums["silver_tip_sum"]
    silver_total_sum = silver_sums["silver_total_sum"]
    gold_fare_sum = gold_sums["gold_fare_sum"]
    gold_tip_sum = gold_sums["gold_tip_sum"]
    gold_total_sum = gold_sums["gold_total_sum"]

    fare_diff = abs(silver_fare_sum - gold_fare_sum)
    tip_diff = abs(silver_tip_sum - gold_tip_sum)
    total_diff = abs(silver_total_sum - gold_total_sum)

    assert fare_diff < 0.01, f"Fare sum mismatch: diff={fare_diff}"
    assert tip_diff < 0.01, f"Tip sum mismatch: diff={tip_diff}"
    assert total_diff < 0.01, f"Total sum mismatch: diff={total_diff}"

    results = {
        "silver_fare_sum": silver_fare_sum,
        "gold_fare_sum": gold_fare_sum,
        "fare_diff": fare_diff,
        "silver_tip_sum": silver_tip_sum,
        "gold_tip_sum": gold_tip_sum,
        "tip_diff": tip_diff,
        "silver_total_sum": silver_total_sum,
        "gold_total_sum": gold_total_sum,
        "total_diff": total_diff,
    }
    logger.info("Gold sum reconciliation results=%s", results)
    return results

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sample Spot Check

# COMMAND ----------

def spot_check_sample_trips() -> DataFrame:
    """Return 5 silver rows alongside their resolved gold dimension labels."""
    silver_sample = (
        spark.table(SILVER_CLEAN_TABLE)
        .filter(F.col("pickup_datetime") >= "2024-03-15")
        .filter(F.col("pickup_datetime") < "2024-03-16")
        .orderBy("pickup_datetime")
        .limit(5)
        .alias("silver")
    )
    fct = spark.table(FCT_TRIPS_TABLE).alias("f")
    pickup_zones = spark.table(DIM_ZONES_TABLE).alias("pz")
    dropoff_zones = spark.table(DIM_ZONES_TABLE).alias("dz")
    dim_date = spark.table(DIM_DATE_TABLE).alias("dd")

    joined_df = (
        silver_sample.join(
            fct,
            (
                (F.col("silver.pickup_datetime") == F.col("f.pickup_datetime"))
                & (F.col("silver.pickup_location_id") == F.col("f.pickup_location_id"))
                & (F.col("silver.dropoff_location_id") == F.col("f.dropoff_location_id"))
                & (F.col("silver.fare_amount") == F.col("f.fare_amount"))
            ),
            "inner",
        )
        .join(
            pickup_zones,
            F.col("f.pickup_zone_sk") == F.col("pz.zone_sk"),
            "left",
        )
        .join(
            dropoff_zones,
            F.col("f.dropoff_zone_sk") == F.col("dz.zone_sk"),
            "left",
        )
        .join(
            dim_date,
            F.col("f.pickup_date_sk") == F.col("dd.date_sk"),
            "left",
        )
    )

    spot_count = joined_df.count()
    assert spot_count > 0, (
        "Sample spot check returned 0 rows; join keys did not match fct_trips"
    )

    return joined_df.select(
        F.col("silver.pickup_datetime").alias("pickup_datetime"),
        F.col("silver.dropoff_datetime").alias("dropoff_datetime"),
        F.col("silver.pickup_location_id").alias("silver_pickup_location_id"),
        F.col("f.pickup_zone_sk").alias("gold_pickup_zone_sk"),
        F.col("pz.zone_name").alias("pickup_zone_name"),
        F.col("silver.dropoff_location_id").alias("silver_dropoff_location_id"),
        F.col("f.dropoff_zone_sk").alias("gold_dropoff_zone_sk"),
        F.col("dz.zone_name").alias("dropoff_zone_name"),
        F.col("silver.fare_amount").alias("fare_amount"),
        F.col("silver.tip_amount").alias("tip_amount"),
        F.col("silver.total_amount").alias("total_amount"),
        F.col("f.pickup_date_sk").alias("gold_pickup_date_sk"),
        F.col("dd.full_date").alias("gold_full_date"),
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Validation

# COMMAND ----------

count_results = reconcile_row_counts()
print(f"Silver rows:        {count_results['silver_count']}")
print(f"Gold fct_trips:     {count_results['gold_count']}")
print(f"Date-filtered rows: {count_results['date_filtered']}")

sum_results = reconcile_sums()
print(f"Fare sum  — Silver: {sum_results['silver_fare_sum']:.2f} | Gold: {sum_results['gold_fare_sum']:.2f} | Diff: {sum_results['fare_diff']:.4f}")
print(f"Tip sum   — Silver: {sum_results['silver_tip_sum']:.2f}  | Gold: {sum_results['gold_tip_sum']:.2f}  | Diff: {sum_results['tip_diff']:.4f}")
print(f"Total sum — Silver: {sum_results['silver_total_sum']:.2f} | Gold: {sum_results['gold_total_sum']:.2f} | Diff: {sum_results['total_diff']:.4f}")

spot_df = spot_check_sample_trips()
display(spot_df)

logger.info("Gold validation complete count_results=%s", count_results)
