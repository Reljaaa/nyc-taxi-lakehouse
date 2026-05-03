# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Batch Ingest - Yellow Taxi

# COMMAND ----------

import logging
import uuid

from src.ingestion.bronze import ingest_yellow_trips_batch


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

LANDING_BASE_PATH = "abfss://landing@nyctaxilakehouse.dfs.core.windows.net"
BRONZE_TABLE = "default.yellow_trips_raw"
YEAR = 2024
MONTHS = range(1, 7)

spark.sql("USE CATALOG bronze")

batch_id = str(uuid.uuid4())
logger.info("Starting bronze batch ingest batch_id=%s", batch_id)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reset Existing Bronze Table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Batch Ingest

# COMMAND ----------

total_rows_written = ingest_yellow_trips_batch(
    spark=spark,
    year=YEAR,
    months=MONTHS,
    landing_base_path=LANDING_BASE_PATH,
    bronze_table=BRONZE_TABLE,
    batch_id=batch_id,
)

logger.info(
    "Bronze batch ingest returned total_rows_written=%s batch_id=%s",
    total_rows_written,
    batch_id,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate Source and Written Row Counts

# COMMAND ----------

expected_total_rows = 0

for month in MONTHS:
    month_text = f"{month:02d}"
    source_path = (
        f"{LANDING_BASE_PATH}/yellow_taxi/{YEAR}/{month_text}/"
        f"yellow_tripdata_{YEAR}-{month_text}.parquet"
    )
    source_row_count = spark.read.parquet(source_path).count()
    expected_total_rows += source_row_count
    logger.info(
        "Validation source row count path=%s row_count=%s",
        source_path,
        source_row_count,
    )

logger.info(
    "Validation totals expected_total_rows=%s total_rows_written=%s",
    expected_total_rows,
    total_rows_written,
)

assert total_rows_written == expected_total_rows, (
    "Total rows written does not match source parquet row count sum: "
    f"written={total_rows_written}, expected={expected_total_rows}"
)
