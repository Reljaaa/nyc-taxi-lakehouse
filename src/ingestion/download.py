"""Download TLC source files into the ADLS Gen2 landing zone."""

import logging
import os
import time
from collections.abc import Iterable
from typing import Any

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TLC_TRIP_DATA_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MAX_ATTEMPTS = 3
CHUNK_SIZE_BYTES = 1024 * 1024


def download_tlc_files(
    year: int,
    months: Iterable[int],
    landing_base_path: str,
    dbutils: Any,
) -> None:
    """Download monthly NYC Yellow Taxi parquet files to ADLS landing storage."""
    for month in months:
        month_text = f"{month:02d}"
        file_name = f"yellow_tripdata_{year}-{month_text}.parquet"
        source_url = f"{TLC_TRIP_DATA_BASE_URL}/{file_name}"
        destination_path = (
            f"{_clean_base_path(landing_base_path)}/yellow_taxi/"
            f"{year}/{month_text}/{file_name}"
        )

        _download_to_adls(source_url, destination_path, file_name, dbutils)


def download_zone_lookup(landing_base_path: str, dbutils: Any) -> None:
    """Download the NYC Taxi zone lookup CSV to ADLS landing storage."""
    file_name = "taxi_zone_lookup.csv"
    destination_path = f"{_clean_base_path(landing_base_path)}/lookups/{file_name}"

    _download_to_adls(ZONE_LOOKUP_URL, destination_path, file_name, dbutils)


def _download_to_adls(
    source_url: str,
    destination_path: str,
    file_name: str,
    dbutils: Any,
) -> None:
    local_path = f"/tmp/{file_name}"
    last_error: Exception | None = None

    for attempt in range(MAX_ATTEMPTS):
        start_time = time.monotonic()

        try:
            logger.info("Starting download file=%s attempt=%s", file_name, attempt + 1)
            file_size_bytes = _download_to_local_file(source_url, local_path)

            dbutils.fs.cp(f"file:{local_path}", destination_path)

            duration_seconds = time.monotonic() - start_time
            logger.info(
                "Completed download file=%s size_mb=%.2f duration_seconds=%.2f "
                "status=success",
                file_name,
                file_size_bytes / (1024 * 1024),
                duration_seconds,
            )
            return
        except Exception as error:
            last_error = error
            duration_seconds = time.monotonic() - start_time
            file_size_bytes = _local_file_size_bytes(local_path)
            _log_attempt_failure(
                file_name,
                attempt,
                file_size_bytes,
                duration_seconds,
                error,
            )
        finally:
            _delete_local_file(local_path)

        if attempt < MAX_ATTEMPTS - 1:
            time.sleep(2**attempt)

    raise RuntimeError(
        f"Failed to download {file_name} after {MAX_ATTEMPTS} attempts"
    ) from last_error


def _download_to_local_file(source_url: str, local_path: str) -> int:
    file_size_bytes = 0

    with requests.get(source_url, stream=True, timeout=60) as response:
        response.raise_for_status()

        with open(local_path, "wb") as local_file:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                if chunk:
                    local_file.write(chunk)
                    file_size_bytes += len(chunk)

    return file_size_bytes


def _log_attempt_failure(
    file_name: str,
    attempt: int,
    file_size_bytes: int,
    duration_seconds: float,
    error: Exception,
) -> None:
    if attempt < MAX_ATTEMPTS - 1:
        logger.warning(
            "Download attempt failed file=%s attempt=%s size_mb=%.2f "
            "duration_seconds=%.2f status=retrying error=%s",
            file_name,
            attempt + 1,
            file_size_bytes / (1024 * 1024),
            duration_seconds,
            error,
            exc_info=True,
        )
        return

    logger.error(
        "Download failed file=%s attempt=%s size_mb=%.2f duration_seconds=%.2f "
        "status=failed error=%s",
        file_name,
        attempt + 1,
        file_size_bytes / (1024 * 1024),
        duration_seconds,
        error,
        exc_info=True,
    )


def _local_file_size_bytes(local_path: str) -> int:
    if os.path.exists(local_path):
        return os.path.getsize(local_path)

    return 0


def _delete_local_file(local_path: str) -> None:
    if os.path.exists(local_path):
        os.remove(local_path)


def _clean_base_path(path: str) -> str:
    return path.rstrip("/")
