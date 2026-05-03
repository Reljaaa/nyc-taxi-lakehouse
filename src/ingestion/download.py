"""Download TLC source files into the ADLS Gen2 landing zone."""

import logging
import time
from collections.abc import Iterable
from urllib.parse import urlparse

import requests
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TLC_TRIP_DATA_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MAX_ATTEMPTS = 3


def download_tlc_files(
    year: int,
    months: Iterable[int],
    landing_base_path: str,
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

        _download_to_adls(source_url, destination_path, file_name)


def download_zone_lookup(landing_base_path: str) -> None:
    """Download the NYC Taxi zone lookup CSV to ADLS landing storage."""
    file_name = "taxi_zone_lookup.csv"
    destination_path = f"{_clean_base_path(landing_base_path)}/lookups/{file_name}"

    _download_to_adls(ZONE_LOOKUP_URL, destination_path, file_name)


def _download_to_adls(
    source_url: str,
    destination_path: str,
    file_name: str,
) -> None:
    last_error: Exception | None = None

    for attempt in range(MAX_ATTEMPTS):
        start_time = time.monotonic()
        data = b""

        try:
            logger.info("Starting download file=%s attempt=%s", file_name, attempt + 1)
            data = _download_file(source_url)
            file_size_bytes = len(data)

            _upload_to_adls(destination_path, data)

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
            file_size_bytes = len(data)
            _log_attempt_failure(
                file_name,
                attempt,
                file_size_bytes,
                duration_seconds,
                error,
            )
        if attempt < MAX_ATTEMPTS - 1:
            time.sleep(2**attempt)

    raise RuntimeError(
        f"Failed to download {file_name} after {MAX_ATTEMPTS} attempts"
    ) from last_error


def _download_file(source_url: str) -> bytes:
    with requests.get(source_url, timeout=60) as response:
        response.raise_for_status()

        return response.content


def _upload_to_adls(destination_path: str, data: bytes) -> None:
    account_name, container_name, file_path = _parse_abfss_path(destination_path)
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential,
    )
    file_system_client = service_client.get_file_system_client(container_name)
    file_client = file_system_client.get_file_client(file_path)

    file_client.upload_data(data, overwrite=True)


def _parse_abfss_path(destination_path: str) -> tuple[str, str, str]:
    parsed_path = urlparse(destination_path)

    if parsed_path.scheme != "abfss":
        raise ValueError(f"Expected abfss path, got: {destination_path}")

    if "@" not in parsed_path.netloc:
        raise ValueError(f"Expected container@account netloc, got: {destination_path}")

    container_name, account_host = parsed_path.netloc.split("@", maxsplit=1)
    account_name = account_host.split(".", maxsplit=1)[0]
    file_path = parsed_path.path.lstrip("/")

    if not container_name or not account_name or not file_path:
        raise ValueError(f"Invalid abfss path: {destination_path}")

    return account_name, container_name, file_path


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


def _clean_base_path(path: str) -> str:
    return path.rstrip("/")
