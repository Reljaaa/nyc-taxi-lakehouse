# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Ingest TLC Files to Landing

# COMMAND ----------
# MAGIC %pip install azure-storage-file-datalake azure-identity

# COMMAND ----------

from src.ingestion.download import download_tlc_files, download_zone_lookup


LANDING_BASE_PATH = "abfss://landing@nyctaxilakehouse.dfs.core.windows.net"
YEAR = 2024
MONTHS = range(1, 7)

# COMMAND ----------

download_tlc_files(
    year=YEAR,
    months=MONTHS,
    landing_base_path=LANDING_BASE_PATH,
)

# COMMAND ----------

download_zone_lookup(
    landing_base_path=LANDING_BASE_PATH,
)
