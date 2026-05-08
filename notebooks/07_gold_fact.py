# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Fact Trips

# COMMAND ----------

import logging

from src.utils.logging import configure_logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


configure_logging()
logger = logging.getLogger(__name__)

SILVER_CLEAN_TABLE = "silver.default.yellow_trips_clean"

DIM_ZONES_TABLE = "gold.default.dim_zones"
DIM_DATE_TABLE = "gold.default.dim_date"
DIM_PAYMENT_TYPE_TABLE = "gold.default.dim_payment_type"
DIM_RATE_CODE_TABLE = "gold.default.dim_rate_code"
DIM_VENDOR_TABLE = "gold.default.dim_vendor"

FCT_TRIPS_TABLE = "gold.default.fct_trips"

SILVER_REQUIRED_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
]

FACT_FK_COLUMNS = [
    "pickup_date_sk",
    "pickup_zone_sk",
    "dropoff_zone_sk",
    "payment_type_sk",
    "rate_code_sk",
    "vendor_sk",
]

FACT_FINAL_COLUMNS = [
    "pickup_date_sk",
    "pickup_zone_sk",
    "dropoff_zone_sk",
    "payment_type_sk",
    "rate_code_sk",
    "vendor_sk",
    "pickup_location_id",
    "dropoff_location_id",
    "pickup_datetime",
    "dropoff_datetime",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "trip_distance",
    "passenger_count",
    "trip_duration_seconds",
    "trip_speed_mph",
    "tip_percentage",
    "pickup_hour",
    "dropoff_hour",
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def assert_silver_schema(silver_df: DataFrame) -> None:
    """Validate that the expected Silver columns exist before building the fact."""
    missing_columns = sorted(set(SILVER_REQUIRED_COLUMNS) - set(silver_df.columns))
    assert not missing_columns, (
        "Silver clean table is missing required columns: "
        f"missing={missing_columns}, found={silver_df.columns}"
    )


def assert_no_null_fk(fct_df: DataFrame) -> dict[str, int]:
    """Validate that every fact surrogate FK is populated."""
    null_counts = {
        fk_column: fct_df.filter(F.col(fk_column).isNull()).count()
        for fk_column in FACT_FK_COLUMNS
    }
    failed_columns = {
        fk_column: null_count
        for fk_column, null_count in null_counts.items()
        if null_count > 0
    }
    assert not failed_columns, f"Fact table contains null FKs: {failed_columns}"
    return null_counts


def write_fact_table(fct_df: DataFrame) -> None:
    """Overwrite the Gold fact table partitioned by pickup date surrogate key."""
    logger.info("Writing Gold fact table table=%s", FCT_TRIPS_TABLE)
    (
        fct_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_date_sk")
        .saveAsTable(FCT_TRIPS_TABLE)
    )
    logger.info("Gold fact table write completed table=%s", FCT_TRIPS_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build fct_trips

# COMMAND ----------

def build_fct_trips() -> DataFrame:
    """Build fct_trips by joining Silver trips to Gold dimensions."""
    silver_df = spark.table(SILVER_CLEAN_TABLE)
    assert_silver_schema(silver_df)

    silver_prepared = (
        silver_df.select(*SILVER_REQUIRED_COLUMNS)
        .withColumn(
            "pickup_location_id_join",
            F.coalesce(F.col("pickup_location_id"), F.lit(0)),
        )
        .withColumn(
            "dropoff_location_id_join",
            F.coalesce(F.col("dropoff_location_id"), F.lit(0)),
        )
        .withColumn(
            "payment_type_join",
            F.coalesce(F.col("payment_type"), F.lit(0)),
        )
        .withColumn(
            "rate_code_id_join",
            F.coalesce(F.col("rate_code_id"), F.lit(0)),
        )
        .withColumn(
            "vendor_id_join",
            F.coalesce(F.col("vendor_id"), F.lit(0)),
        )
        .withColumn(
            "pickup_date_join",
            F.date_format(F.col("pickup_datetime"), "yyyyMMdd").cast("int"),
        )
        .filter(F.col("pickup_date_join").between(20240101, 20241231))
        .withColumn(
            "trip_duration_seconds",
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            ).cast("long"),
        )
        .withColumn(
            "trip_speed_mph",
            F.when(
                F.col("trip_duration_seconds") > 0,
                F.col("trip_distance") / (F.col("trip_duration_seconds") / F.lit(3600.0)),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                (F.col("tip_amount") / F.col("fare_amount")) * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("pickup_hour", F.hour("pickup_datetime").cast("int"))
        .withColumn("dropoff_hour", F.hour("dropoff_datetime").cast("int"))
    )

    silver = silver_prepared.alias("silver")
    dim_zones_pickup = spark.table(DIM_ZONES_TABLE).alias("dz_pickup")
    dim_zones_dropoff = spark.table(DIM_ZONES_TABLE).alias("dz_dropoff")
    dim_date = spark.table(DIM_DATE_TABLE).alias("dd")
    dim_payment_type = spark.table(DIM_PAYMENT_TYPE_TABLE).alias("dpt")
    dim_rate_code = spark.table(DIM_RATE_CODE_TABLE).alias("drc")
    dim_vendor = spark.table(DIM_VENDOR_TABLE).alias("dv")

    joined_df = (
        silver.join(
            dim_zones_pickup,
            F.col("silver.pickup_location_id_join") == F.col("dz_pickup.location_id"),
            "left",
        )
        .join(
            dim_zones_dropoff,
            F.col("silver.dropoff_location_id_join") == F.col("dz_dropoff.location_id"),
            "left",
        )
        .join(
            dim_date,
            F.col("silver.pickup_date_join") == F.col("dd.date_sk"),
            "left",
        )
        .join(
            dim_payment_type,
            F.col("silver.payment_type_join") == F.col("dpt.payment_type_id"),
            "left",
        )
        .join(
            dim_rate_code,
            F.col("silver.rate_code_id_join") == F.col("drc.rate_code_id"),
            "left",
        )
        .join(
            dim_vendor,
            F.col("silver.vendor_id_join") == F.col("dv.vendor_id"),
            "left",
        )
    )

    return joined_df.select(
        F.col("dd.date_sk").alias("pickup_date_sk"),
        F.coalesce(F.col("dz_pickup.zone_sk"), F.lit(0)).cast("bigint").alias("pickup_zone_sk"),
        F.coalesce(F.col("dz_dropoff.zone_sk"), F.lit(0)).cast("bigint").alias("dropoff_zone_sk"),
        F.coalesce(F.col("dpt.payment_type_sk"), F.lit(0)).cast("bigint").alias("payment_type_sk"),
        F.coalesce(F.col("drc.rate_code_sk"), F.lit(0)).cast("bigint").alias("rate_code_sk"),
        F.coalesce(F.col("dv.vendor_sk"), F.lit(0)).cast("bigint").alias("vendor_sk"),
        F.col("silver.pickup_location_id").alias("pickup_location_id"),
        F.col("silver.dropoff_location_id").alias("dropoff_location_id"),
        F.col("silver.pickup_datetime").alias("pickup_datetime"),
        F.col("silver.dropoff_datetime").alias("dropoff_datetime"),
        F.col("silver.fare_amount").alias("fare_amount"),
        F.col("silver.tip_amount").alias("tip_amount"),
        F.col("silver.tolls_amount").alias("tolls_amount"),
        F.col("silver.total_amount").alias("total_amount"),
        F.col("silver.trip_distance").alias("trip_distance"),
        F.col("silver.passenger_count").alias("passenger_count"),
        F.col("silver.trip_duration_seconds").alias("trip_duration_seconds"),
        F.col("silver.trip_speed_mph").alias("trip_speed_mph"),
        F.col("silver.tip_percentage").alias("tip_percentage"),
        F.col("silver.pickup_hour").alias("pickup_hour"),
        F.col("silver.dropoff_hour").alias("dropoff_hour"),
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write and Validate fct_trips

# COMMAND ----------

fct_trips_df = build_fct_trips()
assert fct_trips_df.columns == FACT_FINAL_COLUMNS, (
    f"Unexpected fct_trips column order: {fct_trips_df.columns}"
)

silver_row_count = spark.table(SILVER_CLEAN_TABLE).count()
pre_write_fact_count = fct_trips_df.count()
date_filtered_rows = silver_row_count - pre_write_fact_count
assert date_filtered_rows >= 0, (
    f"Fact has MORE rows than Silver — unexpected fan-out: "
    f"fact={pre_write_fact_count}, silver={silver_row_count}"
)
assert date_filtered_rows <= 100, (
    f"Too many Silver rows filtered out at date boundary: "
    f"filtered={date_filtered_rows}, silver={silver_row_count}, fact={pre_write_fact_count}"
)
if date_filtered_rows > 0:
    logger.info(
        "Date-boundary filter removed rows outside 2024 filtered_count=%s silver=%s fact=%s",
        date_filtered_rows, silver_row_count, pre_write_fact_count,
    )
pre_write_null_fk_counts = assert_no_null_fk(fct_trips_df)

write_fact_table(fct_trips_df)

written_fct_df = spark.table(FCT_TRIPS_TABLE)
fact_row_count = written_fct_df.count()
assert fact_row_count == pre_write_fact_count, (
    "Fact row count changed between pre-write and post-write: "
    f"pre={pre_write_fact_count}, post={fact_row_count}"
)
post_write_null_fk_counts = assert_no_null_fk(written_fct_df)

logger.info(
    "fct_trips validation passed silver_count=%s fact_count=%s fk_null_counts=%s",
    silver_row_count,
    fact_row_count,
    post_write_null_fk_counts,
)

print(f"Silver rows: {silver_row_count}")
print(f"Date-boundary filtered: {date_filtered_rows}")
print(f"fct_trips rows: {fact_row_count}")
print(f"Pre-write FK null counts: {pre_write_null_fk_counts}")
print(f"Post-write FK null counts: {post_write_null_fk_counts}")
written_fct_df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Spot Check Dimension Joins

# COMMAND ----------

spot_check_df = (
    written_fct_df.alias("f")
    .join(
        spark.table(DIM_ZONES_TABLE).alias("pickup_zone"),
        F.col("f.pickup_zone_sk") == F.col("pickup_zone.zone_sk"),
        "left",
    )
    .join(
        spark.table(DIM_ZONES_TABLE).alias("dropoff_zone"),
        F.col("f.dropoff_zone_sk") == F.col("dropoff_zone.zone_sk"),
        "left",
    )
    .join(
        spark.table(DIM_DATE_TABLE).alias("pickup_date"),
        F.col("f.pickup_date_sk") == F.col("pickup_date.date_sk"),
        "left",
    )
    .join(
        spark.table(DIM_PAYMENT_TYPE_TABLE).alias("payment_type"),
        F.col("f.payment_type_sk") == F.col("payment_type.payment_type_sk"),
        "left",
    )
    .join(
        spark.table(DIM_RATE_CODE_TABLE).alias("rate_code"),
        F.col("f.rate_code_sk") == F.col("rate_code.rate_code_sk"),
        "left",
    )
    .join(
        spark.table(DIM_VENDOR_TABLE).alias("vendor"),
        F.col("f.vendor_sk") == F.col("vendor.vendor_sk"),
        "left",
    )
    .select(
        F.col("f.pickup_datetime"),
        F.col("f.pickup_date_sk"),
        F.col("pickup_date.full_date").alias("pickup_full_date"),
        F.col("f.pickup_location_id"),
        F.col("f.pickup_zone_sk"),
        F.col("pickup_zone.zone_name").alias("pickup_zone_name"),
        F.col("f.dropoff_location_id"),
        F.col("f.dropoff_zone_sk"),
        F.col("dropoff_zone.zone_name").alias("dropoff_zone_name"),
        F.col("f.payment_type_sk"),
        F.col("payment_type.payment_type_description"),
        F.col("f.rate_code_sk"),
        F.col("rate_code.rate_code_description"),
        F.col("f.vendor_sk"),
        F.col("vendor.vendor_name"),
    )
    .limit(5)
)

display(spot_check_df)
