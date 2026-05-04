# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Ingest TLC Files to Landing

# COMMAND ----------

import logging

from src.ingestion.download import download_tlc_files, download_zone_lookup
from src.utils.logging import configure_logging


configure_logging()
logger = logging.getLogger(__name__)

LANDING_BASE_PATH = "abfss://landing@nyctaxilakehouse.dfs.core.windows.net"
YEAR = 2024
MONTHS = range(1, 7)

current_user = spark.sql("SELECT current_user()").first()[0]
STAGING_DIR = f"/Workspace/Users/{current_user}/_tmp_staging"

# COMMAND ----------

download_tlc_files(
    year=YEAR,
    months=MONTHS,
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
