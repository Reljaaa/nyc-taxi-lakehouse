# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Batch Ingest - Yellow Taxi

# COMMAND ----------

import logging
import uuid

from src.ingestion.bronze import (
    backfill_control_table_from_bronze,
    ensure_ingested_files_table,
    filter_unprocessed_months,
    get_landing_file_size,
    ingest_single_month,
    record_ingested_file,
)
from src.utils.logging import configure_logging


configure_logging()
logger = logging.getLogger(__name__)

LANDING_BASE_PATH = "abfss://landing@nyctaxilakehouse.dfs.core.windows.net"
BRONZE_TABLE = "default.yellow_trips_raw"
CONTROL_TABLE = "default.ingested_files"
YEAR = 2024
MONTHS = range(1, 7)

spark.sql("USE CATALOG bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ensure Control Table

# COMMAND ----------

ensure_ingested_files_table(spark, CONTROL_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Backfill Control Table

# COMMAND ----------

backfilled_rows = backfill_control_table_from_bronze(
    spark=spark,
    dbutils=dbutils,
    bronze_table=BRONZE_TABLE,
    control_table=CONTROL_TABLE,
)
logger.info("Control table backfill completed rows=%s", backfilled_rows)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Filter Already Ingested Files

# COMMAND ----------

months_to_ingest = filter_unprocessed_months(
    spark=spark,
    year=YEAR,
    candidate_months=MONTHS,
    landing_base_path=LANDING_BASE_PATH,
    control_table=CONTROL_TABLE,
)
logger.info("Months queued for ingest months=%s", months_to_ingest)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Missing File Ingest

# COMMAND ----------

if not months_to_ingest:
    logger.info("All files already ingested - nothing to do")
else:
    batch_id = str(uuid.uuid4())
    logger.info("Starting bronze batch ingest batch_id=%s", batch_id)

    for month in months_to_ingest:
        source_path, row_count = ingest_single_month(
            spark=spark,
            year=YEAR,
            month=month,
            landing_base_path=LANDING_BASE_PATH,
            bronze_table=BRONZE_TABLE,
            batch_id=batch_id,
        )
        file_size_bytes = get_landing_file_size(dbutils, source_path)

        # Atomicity gap: if this fails, data is already in bronze
        try:
            record_ingested_file(
                spark=spark,
                control_table=CONTROL_TABLE,
                source_file=source_path,
                file_size_bytes=file_size_bytes,
                ingestion_batch_id=batch_id,
                row_count=row_count,
            )
        except Exception as error:
            logger.error(
                "Failed to record ingested file source_file=%s batch_id=%s error=%s",
                source_path,
                batch_id,
                error,
                exc_info=True,
            )
            raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate Bronze and Control Row Counts

# COMMAND ----------

bronze_row_count = spark.table(BRONZE_TABLE).count()
control_total_row_count = spark.sql(
    f"SELECT COALESCE(SUM(row_count), 0) AS total_rows FROM {CONTROL_TABLE}"
).first()["total_rows"]

logger.info(
    "Validation totals bronze_row_count=%s control_total_row_count=%s",
    bronze_row_count,
    control_total_row_count,
)

assert bronze_row_count == control_total_row_count, (
    "Bronze row count does not match control table row count sum: "
    f"bronze={bronze_row_count}, control_sum={control_total_row_count}"
)
