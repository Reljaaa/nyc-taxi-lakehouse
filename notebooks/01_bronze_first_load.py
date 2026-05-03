# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze First Load - January 2024 Yellow Taxi

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read January source file

# COMMAND ----------

SOURCE_PATH = (
    "abfss://landing@nyctaxilakehouse.dfs.core.windows.net/"
    "yellow_taxi/2024/01/yellow_tripdata_2024-01.parquet"
)

yellow_january_df = spark.read.parquet(SOURCE_PATH)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inspect source data

# COMMAND ----------

yellow_january_df.printSchema()

source_row_count = yellow_january_df.count()
print(f"Source row count: {source_row_count}")

yellow_january_df.show(5)

yellow_january_df.describe(
    "fare_amount",
    "trip_distance",
    "passenger_count",
).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Add partition columns

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType


yellow_january_partitioned_df = (
    yellow_january_df
    .withColumn("year", lit(2024).cast(IntegerType()))
    .withColumn("month", lit(1).cast(IntegerType()))
)

yellow_january_partitioned_df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write bronze Delta table

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

(
    yellow_january_partitioned_df.write
    .format("delta")
    .mode("append")
    .partitionBy("year", "month")
    .saveAsTable("bronze.yellow_trips_raw")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify bronze write

# COMMAND ----------

bronze_df = spark.table("bronze.yellow_trips_raw")
bronze_row_count = bronze_df.count()

print(f"Source row count: {source_row_count}")
print(f"Bronze row count: {bronze_row_count}")

assert bronze_row_count == source_row_count, (
    "Bronze row count does not match source row count: "
    f"source={source_row_count}, bronze={bronze_row_count}"
)

bronze_df.printSchema()
bronze_df.show(5)
