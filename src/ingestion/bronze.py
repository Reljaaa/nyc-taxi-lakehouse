"""Bronze ingestion helpers for NYC Yellow Taxi trip data."""

import logging
from collections.abc import Iterable
from typing import Any

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import IntegerType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def ingest_yellow_trips_batch(
    spark: Any,
    year: int,
    months: Iterable[int],
    landing_base_path: str,
    bronze_table: str,
    batch_id: str,
) -> int:
    """Append monthly Yellow Taxi parquet files to the bronze Delta table."""
    total_rows_written = 0

    for month in months:
        source_path = _yellow_taxi_source_path(landing_base_path, year, month)
        logger.info("Processing source file path=%s", source_path)

        source_df = spark.read.parquet(source_path)
        row_count = source_df.count()
        logger.info(
            "Source file row count path=%s row_count=%s",
            source_path,
            row_count,
        )

        bronze_df = (
            source_df
            .withColumn("year", lit(year).cast(IntegerType()))
            .withColumn("month", lit(month).cast(IntegerType()))
            .withColumn("_source_file", lit(source_path))
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_ingestion_batch_id", lit(batch_id))
        )

        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .partitionBy("year", "month")
            .saveAsTable(bronze_table)
        )

        total_rows_written += row_count

    logger.info("Completed bronze batch ingest total_rows=%s", total_rows_written)

    return total_rows_written


def _yellow_taxi_source_path(
    landing_base_path: str,
    year: int,
    month: int,
) -> str:
    month_text = f"{month:02d}"
    clean_base_path = landing_base_path.rstrip("/")

    return (
        f"{clean_base_path}/yellow_taxi/{year}/{month_text}/"
        f"yellow_tripdata_{year}-{month_text}.parquet"
    )
