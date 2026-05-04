# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Validation

# COMMAND ----------

import logging

from src.utils.logging import configure_logging


configure_logging()
logger = logging.getLogger(__name__)

BRONZE_TABLE = "default.yellow_trips_raw"
CONTROL_TABLE = "default.ingested_files"
LANDING_BASE_PATH = "abfss://landing@nyctaxilakehouse.dfs.core.windows.net"
EXPECTED_YEAR = 2024
EXPECTED_MONTHS = list(range(1, 7))

spark.sql("USE CATALOG bronze")


def _assert_set_equal(actual: set, expected: set, label: str) -> None:
    missing = expected - actual
    unexpected = actual - expected
    assert actual == expected, (
        f"{label} mismatch — missing={sorted(missing)} "
        f"unexpected={sorted(unexpected)}"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## A1 — Bronze Table Not Empty

# COMMAND ----------

total_count = spark.table(BRONZE_TABLE).count()
logger.info("A1 bronze row count count=%s", total_count)
assert total_count > 0, "Bronze table is empty"

# COMMAND ----------
# MAGIC %md
# MAGIC ## A2 — Bronze Count Equals Control Table Sum

# COMMAND ----------

control_sum = spark.sql(
    f"SELECT COALESCE(SUM(row_count), 0) AS s FROM {CONTROL_TABLE}"
).first()["s"]
logger.info("A2 bronze=%s control_sum=%s", total_count, control_sum)
assert total_count == control_sum, f"bronze={total_count} != control_sum={control_sum}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## A3 — Schema Check

# COMMAND ----------

expected_metadata_types = {
    "year": "IntegerType()",
    "month": "IntegerType()",
    "_source_file": "StringType()",
    "_ingestion_timestamp": "TimestampType()",
    "_ingestion_batch_id": "StringType()",
}

bronze_schema = spark.table(BRONZE_TABLE).schema
actual_columns = {field.name for field in bronze_schema}
field_by_name = {field.name: field for field in bronze_schema}

for column_name, expected_type_str in expected_metadata_types.items():
    assert column_name in field_by_name, f"Missing metadata column: {column_name}"
    actual_type_str = str(field_by_name[column_name].dataType)
    assert actual_type_str == expected_type_str, (
        f"Metadata column {column_name} type mismatch: "
        f"actual={actual_type_str}, expected={expected_type_str}"
    )

required_tlc_columns = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "VendorID",
    "RatecodeID",
}
missing = required_tlc_columns - actual_columns
assert not missing, f"Missing required TLC columns: {sorted(missing)}"

logger.info("A3 schema checks passed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## A4 — Source Files Match Expected Paths

# COMMAND ----------

expected_source_files = {
    f"{LANDING_BASE_PATH}/yellow_taxi/{EXPECTED_YEAR}/{month:02d}/"
    f"yellow_tripdata_{EXPECTED_YEAR}-{month:02d}.parquet"
    for month in EXPECTED_MONTHS
}
actual_source_files = {
    row["_source_file"]
    for row in spark.table(BRONZE_TABLE).select("_source_file").distinct().collect()
}
_assert_set_equal(actual_source_files, expected_source_files, "A4 source files")
logger.info("A4 source files set match passed count=%s", len(actual_source_files))

# COMMAND ----------
# MAGIC %md
# MAGIC ## A5 — Year/Month Partitions Match Expected Months

# COMMAND ----------

expected_year_months = {(EXPECTED_YEAR, month) for month in EXPECTED_MONTHS}
actual_year_months = {
    (row["year"], row["month"])
    for row in spark.table(BRONZE_TABLE).select("year", "month").distinct().collect()
}
_assert_set_equal(actual_year_months, expected_year_months, "A5 year/month partitions")
logger.info("A5 year/month set match passed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## A6 — No Null Pickup Datetimes

# COMMAND ----------

null_pickup_count = spark.sql(
    f"SELECT COUNT(*) AS c FROM {BRONZE_TABLE} WHERE tpep_pickup_datetime IS NULL"
).first()["c"]
logger.info("A6 null pickup datetime count=%s", null_pickup_count)
assert null_pickup_count == 0, (
    f"Found {null_pickup_count} rows with NULL tpep_pickup_datetime"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## A7 — Rogue Pickup Date Warning

# COMMAND ----------

out_of_range_count = spark.sql(
    f"""
    SELECT COUNT(*) AS c FROM {BRONZE_TABLE}
    WHERE NOT (
        year(tpep_pickup_datetime) = {EXPECTED_YEAR}
        AND month(tpep_pickup_datetime) BETWEEN 1 AND 6
    )
    """
).first()["c"]
out_of_range_pct = (out_of_range_count / total_count) * 100 if total_count else 0.0

# Soft check: bronze does not clean rogue dates; silver DQ rules will quarantine them in Phase 3.
if out_of_range_count > 0:
    logger.warning(
        "A7 rogue pickup datetimes (out of expected Jan-Jun 2024) count=%s pct=%.4f",
        out_of_range_count,
        out_of_range_pct,
    )
else:
    logger.info("A7 no rogue pickup datetimes")

# COMMAND ----------
# MAGIC %md
# MAGIC All bronze validation checks passed.

# COMMAND ----------

logger.info("Bronze validation complete checks_passed=7")
