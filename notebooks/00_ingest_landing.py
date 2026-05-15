# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Ingest TLC Files to Landing

# COMMAND ----------

import logging

from src.ingestion.download import download_tlc_files, download_zone_lookup
from src.utils.logging import configure_logging
from src.utils.params import parse_month_range


configure_logging()
logger = logging.getLogger(__name__)

dbutils.widgets.text("start_month", "2024-01")
dbutils.widgets.text("end_month", "2024-06")

start_month = dbutils.widgets.get("start_month")
end_month   = dbutils.widgets.get("end_month")
parsed_month_range = parse_month_range(start_month, end_month)
year = parsed_month_range[0][0]
months = [month for _, month in parsed_month_range]
if any(parsed_year != year for parsed_year, _ in parsed_month_range):
    logger.warning(
        "Landing ingest assumes a single-year month range start_month=%s end_month=%s",
        start_month,
        end_month,
    )

LANDING_BASE_PATH = "abfss://landing@nyctaxilakehouse.dfs.core.windows.net"

current_user = spark.sql("SELECT current_user()").first()[0]
STAGING_DIR = f"/Workspace/Users/{current_user}/_tmp_staging"

# COMMAND ----------

download_tlc_files(
    # download_tlc_files accepts one year plus month numbers; this assumes a single-year range.
    year=year,
    months=months,
    landing_base_path=LANDING_BASE_PATH,
    dbutils=dbutils,
    staging_dir=STAGING_DIR,
)

# COMMAND ----------

download_zone_lookup(
    landing_base_path=LANDING_BASE_PATH,
    dbutils=dbutils,
    staging_dir=STAGING_DIR,
)
