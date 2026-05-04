"""Bronze ingestion helpers for NYC Yellow Taxi trip data."""

import logging
from collections.abc import Iterable
from typing import Any

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def ensure_ingested_files_table(spark: Any, control_table: str) -> None:
    """Create the ingested files control table if it does not exist."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {control_table} (
            source_file STRING,
            file_size_bytes BIGINT,
            ingested_at TIMESTAMP,
            ingestion_batch_id STRING,
            row_count BIGINT
        )
        USING DELTA
        """
    )
    logger.info("Ensured control table exists table=%s", control_table)


def filter_unprocessed_months(
    spark: Any,
    year: int,
    candidate_months: Iterable[int],
    landing_base_path: str,
    control_table: str,
) -> list[int]:
    """Return months whose source files are not yet in the control table."""
    ingested_files = {
        row.source_file
        for row in spark.table(control_table).select("source_file").collect()
    }
    months_to_ingest = []

    for month in candidate_months:
        source_path = _yellow_taxi_source_path(landing_base_path, year, month)

        if source_path in ingested_files:
            logger.info("Skipping already ingested source file path=%s", source_path)
            continue

        logger.info("Queueing source file for ingest path=%s", source_path)
        months_to_ingest.append(month)

    return months_to_ingest


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
        _, row_count = ingest_single_month(
            spark=spark,
            year=year,
            month=month,
            landing_base_path=landing_base_path,
            bronze_table=bronze_table,
            batch_id=batch_id,
        )
        total_rows_written += row_count

    logger.info("Completed bronze batch ingest total_rows=%s", total_rows_written)

    return total_rows_written


def ingest_single_month(
    spark: Any,
    year: int,
    month: int,
    landing_base_path: str,
    bronze_table: str,
    batch_id: str,
) -> tuple[str, int]:
    """Append one monthly Yellow Taxi parquet file to the bronze Delta table."""
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

    return source_path, row_count


def record_ingested_file(
    spark: Any,
    control_table: str,
    source_file: str,
    file_size_bytes: int,
    ingestion_batch_id: str,
    row_count: int,
) -> None:
    """Append one source file record to the ingested files control table."""
    schema = _control_table_insert_schema()
    control_df = spark.createDataFrame(
        [(source_file, file_size_bytes, ingestion_batch_id, row_count)],
        schema,
    ).withColumn("ingested_at", current_timestamp())

    control_df.select(
        "source_file",
        "file_size_bytes",
        "ingested_at",
        "ingestion_batch_id",
        "row_count",
    ).write.format("delta").mode("append").saveAsTable(control_table)

    logger.info(
        "Recorded ingested source file path=%s row_count=%s file_size_bytes=%s",
        source_file,
        row_count,
        file_size_bytes,
    )


def get_landing_file_size(dbutils: Any, source_path: str) -> int:
    """Return the ADLS landing file size in bytes."""
    parent_dir, file_name = source_path.rsplit("/", maxsplit=1)

    for entry in dbutils.fs.ls(parent_dir):
        if entry.name.rstrip("/") == file_name:
            if entry.size is None:
                raise ValueError(f"File size not available for {source_path}")

            return int(entry.size)

    raise FileNotFoundError(f"Landing file not found: {source_path}")


def backfill_control_table_from_bronze(
    spark: Any,
    dbutils: Any,
    bronze_table: str,
    control_table: str,
) -> int:
    """Backfill the control table from existing bronze metadata if needed."""
    control_row_count = spark.table(control_table).count()

    if control_row_count > 0:
        logger.info(
            "Skipping control table backfill because control table has rows count=%s",
            control_row_count,
        )
        return 0

    bronze_row_count = spark.table(bronze_table).count()

    if bronze_row_count == 0:
        logger.info("Skipping control table backfill because bronze table is empty")
        return 0

    grouped_rows = spark.sql(
        f"""
        SELECT
            _source_file AS source_file,
            MIN(_ingestion_timestamp) AS ingested_at,
            MIN(_ingestion_batch_id) AS ingestion_batch_id,
            COUNT(*) AS row_count
        FROM {bronze_table}
        GROUP BY _source_file
        """
    ).collect()

    backfill_rows = [
        (
            row.source_file,
            get_landing_file_size(dbutils, row.source_file),
            row.ingested_at,
            row.ingestion_batch_id,
            row.row_count,
        )
        for row in grouped_rows
    ]

    if not backfill_rows:
        logger.info("Skipping control table backfill because no source files were found")
        return 0

    spark.createDataFrame(
        backfill_rows,
        _control_table_schema(),
    ).write.format("delta").mode("append").saveAsTable(control_table)

    backfilled_count = len(backfill_rows)
    logger.info(
        "Backfilled control table rows=%s table=%s",
        backfilled_count,
        control_table,
    )

    return backfilled_count


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


def _control_table_schema() -> StructType:
    return StructType(
        [
            StructField("source_file", StringType(), nullable=False),
            StructField("file_size_bytes", LongType(), nullable=False),
            StructField("ingested_at", TimestampType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
            StructField("row_count", LongType(), nullable=False),
        ]
    )


def _control_table_insert_schema() -> StructType:
    return StructType(
        [
            StructField("source_file", StringType(), nullable=False),
            StructField("file_size_bytes", LongType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
            StructField("row_count", LongType(), nullable=False),
        ]
    )
