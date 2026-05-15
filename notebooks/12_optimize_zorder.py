# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # OPTIMIZE + Z-ORDER Benchmark
# MAGIC
# MAGIC One-time analysis notebook for capturing before/after Delta table metrics, benchmark timings, and explain plans for the Silver and Gold performance tuning pass.

# COMMAND ----------

import logging
import time

from src.utils.logging import configure_logging

from pyspark.sql import functions as F


configure_logging()
logger = logging.getLogger(__name__)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

SILVER_CLEAN_TABLE = "silver.default.yellow_trips_clean"
GOLD_FCT_TRIPS_TABLE = "gold.default.fct_trips"
GOLD_DAILY_METRICS_TABLE = "gold.default.daily_metrics_by_zone"
GOLD_HOURLY_PATTERNS_TABLE = "gold.default.hourly_demand_patterns"
DIM_ZONES_TABLE = "gold.default.dim_zones"

BENCHMARK_ZONE_LOCATION_IDS = [132, 138, 161, 230]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def capture_table_metrics(table_fqn: str) -> dict:
    """Capture Delta table file, size, partition, and Z-ORDER metadata."""
    logger.info("Capturing table metrics table=%s", table_fqn)
    row = spark.sql(f"DESCRIBE DETAIL {table_fqn}").first().asDict()
    return {
        "numFiles": row.get("numFiles"),
        "sizeInBytes": row.get("sizeInBytes"),
        "partitionColumns": row.get("partitionColumns") or [],
        "zOrderBy": row.get("zOrderBy") or row.get("zorderBy") or [],
    }


def _median(values: list[float]) -> float:
    """Return the median for a non-empty list of float values."""
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2


def run_benchmark_query(sql_text: str, label: str, runs: int = 3) -> dict:
    """Run a benchmark query multiple times and return timing statistics."""
    logger.info("Starting benchmark label=%s runs=%s", label, runs)
    elapsed_runs = []
    for run_number in range(runs):
        # clearCache cannot flush Databricks storage-layer disk cache from notebook code.
        spark.catalog.clearCache()
        started_at = time.time()
        spark.sql(sql_text).collect()
        elapsed = time.time() - started_at
        elapsed_runs.append(elapsed)
        logger.info(
            "Benchmark run completed label=%s run=%s elapsed_seconds=%.4f",
            label,
            run_number + 1,
            elapsed,
        )

    return {
        "label": label,
        "runs": elapsed_runs,
        "median": _median(elapsed_runs),
        "min": min(elapsed_runs),
        "max": max(elapsed_runs),
    }


def capture_explain_formatted(sql_text: str) -> str:
    """Capture EXPLAIN FORMATTED output for a SQL query."""
    logger.info("Capturing EXPLAIN FORMATTED output")
    rows = spark.sql(f"EXPLAIN FORMATTED {sql_text}").collect()
    return "\n".join(row[0] for row in rows)


def run_optimize(optimize_sql: str, table_fqn: str) -> float:
    """Run one OPTIMIZE statement and return elapsed seconds."""
    logger.info("Starting OPTIMIZE table=%s", table_fqn)
    started_at = time.time()
    try:
        spark.sql(optimize_sql).collect()
    except Exception as error:
        logger.error(
            "OPTIMIZE failed table=%s sql=%s error=%s",
            table_fqn,
            optimize_sql,
            error,
            exc_info=True,
        )
        raise
    elapsed = time.time() - started_at
    logger.info("OPTIMIZE completed table=%s elapsed_seconds=%.4f", table_fqn, elapsed)
    return elapsed


def print_comparison(
    table_fqn: str,
    before_metrics: dict,
    before_bench: dict | None,
    after_metrics: dict,
    after_bench: dict | None,
) -> None:
    """Print a compact before/after comparison block."""
    print(f"=== {table_fqn} ===")
    print(
        f"numFiles       before={before_metrics['numFiles']}   "
        f"after={after_metrics['numFiles']}"
    )
    print(
        f"sizeInBytes    before={before_metrics['sizeInBytes']}   "
        f"after={after_metrics['sizeInBytes']}"
    )
    print(
        f"partitions     before={before_metrics['partitionColumns']}   "
        f"after={after_metrics['partitionColumns']}"
    )
    print(
        f"zOrderBy       before={before_metrics['zOrderBy']}   "
        f"after={after_metrics['zOrderBy']}"
    )
    if before_bench and after_bench:
        before_median = before_bench["median"]
        after_median = after_bench["median"]
        delta_pct = (
            ((after_median - before_median) / before_median) * 100
            if before_median
            else 0.0
        )
        print(
            f"bench median   before={before_median:.4f}s  "
            f"after={after_median:.4f}s   (delta = {delta_pct:.2f}%)"
        )
        print(
            f"bench runs     before={[round(v, 4) for v in before_bench['runs']]}   "
            f"after={[round(v, 4) for v in after_bench['runs']]}"
        )
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resolve Benchmark Zone Keys

# COMMAND ----------

zone_key_rows = (
    spark.table(DIM_ZONES_TABLE)
    .filter(F.col("location_id").isin(BENCHMARK_ZONE_LOCATION_IDS))
    .select("location_id", "zone_sk")
    .orderBy("location_id")
    .collect()
)
location_to_zone_sk = {
    row["location_id"]: row["zone_sk"]
    for row in zone_key_rows
}
missing_location_ids = sorted(
    set(BENCHMARK_ZONE_LOCATION_IDS) - set(location_to_zone_sk.keys())
)
assert not missing_location_ids, (
    f"Missing dim_zones surrogate keys for location_ids={missing_location_ids}"
)
benchmark_zone_sks = [
    location_to_zone_sk[location_id]
    for location_id in BENCHMARK_ZONE_LOCATION_IDS
]
logger.info(
    "Resolved benchmark zone surrogate keys location_to_zone_sk=%s",
    location_to_zone_sk,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Benchmark SQL

# COMMAND ----------

silver_zone_ids_sql = ", ".join(str(location_id) for location_id in BENCHMARK_ZONE_LOCATION_IDS)
gold_zone_sks_sql = ", ".join(str(zone_sk) for zone_sk in benchmark_zone_sks)

SILVER_BENCH_SQL = f"""
SELECT
    pickup_location_id,
    COUNT(*) AS trips,
    SUM(fare_amount) AS revenue
FROM {SILVER_CLEAN_TABLE}
WHERE pickup_location_id IN ({silver_zone_ids_sql})
GROUP BY pickup_location_id
"""

GOLD_BENCH_SQL = f"""
SELECT
    pickup_zone_sk,
    COUNT(*) AS trips,
    SUM(fare_amount) AS revenue
FROM {GOLD_FCT_TRIPS_TABLE}
WHERE pickup_zone_sk IN ({gold_zone_sks_sql})
GROUP BY pickup_zone_sk
"""

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Before

# COMMAND ----------

silver_before_metrics = capture_table_metrics(SILVER_CLEAN_TABLE)
silver_before_bench = run_benchmark_query(SILVER_BENCH_SQL, "silver_before", runs=3)
silver_before_explain = capture_explain_formatted(SILVER_BENCH_SQL)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver OPTIMIZE + Z-ORDER

# COMMAND ----------

silver_optimize_seconds = run_optimize(
    f"OPTIMIZE {SILVER_CLEAN_TABLE} ZORDER BY (pickup_location_id, dropoff_location_id)",
    SILVER_CLEAN_TABLE,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver After

# COMMAND ----------

silver_after_metrics = capture_table_metrics(SILVER_CLEAN_TABLE)
silver_after_bench = run_benchmark_query(SILVER_BENCH_SQL, "silver_after", runs=3)
silver_after_explain = capture_explain_formatted(SILVER_BENCH_SQL)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Fact Before

# COMMAND ----------

gold_before_metrics = capture_table_metrics(GOLD_FCT_TRIPS_TABLE)
gold_before_bench = run_benchmark_query(GOLD_BENCH_SQL, "gold_before", runs=3)
gold_before_explain = capture_explain_formatted(GOLD_BENCH_SQL)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Fact OPTIMIZE + Z-ORDER

# COMMAND ----------

gold_optimize_seconds = run_optimize(
    # pickup_date_sk is the partition column, so Z-ORDER targets non-partition zone keys.
    f"OPTIMIZE {GOLD_FCT_TRIPS_TABLE} ZORDER BY (pickup_zone_sk, dropoff_zone_sk)",
    GOLD_FCT_TRIPS_TABLE,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Fact After

# COMMAND ----------

gold_after_metrics = capture_table_metrics(GOLD_FCT_TRIPS_TABLE)
gold_after_bench = run_benchmark_query(GOLD_BENCH_SQL, "gold_after", runs=3)
gold_after_explain = capture_explain_formatted(GOLD_BENCH_SQL)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pre-Aggregates OPTIMIZE Only

# COMMAND ----------

daily_before_metrics = capture_table_metrics(GOLD_DAILY_METRICS_TABLE)
hourly_before_metrics = capture_table_metrics(GOLD_HOURLY_PATTERNS_TABLE)

daily_optimize_seconds = run_optimize(
    f"OPTIMIZE {GOLD_DAILY_METRICS_TABLE}",
    GOLD_DAILY_METRICS_TABLE,
)
hourly_optimize_seconds = run_optimize(
    f"OPTIMIZE {GOLD_HOURLY_PATTERNS_TABLE}",
    GOLD_HOURLY_PATTERNS_TABLE,
)

daily_after_metrics = capture_table_metrics(GOLD_DAILY_METRICS_TABLE)
hourly_after_metrics = capture_table_metrics(GOLD_HOURLY_PATTERNS_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Consolidated Report

# COMMAND ----------

print("OPTIMIZE + Z-ORDER comparison")
print("Storage-layer disk cache cannot be flushed from notebook code; benchmark timing may still include warm-cache effects.")
print()

print(f"Silver OPTIMIZE seconds: {silver_optimize_seconds:.2f}")
print(f"Gold fact OPTIMIZE seconds: {gold_optimize_seconds:.2f}")
print(f"Daily metrics OPTIMIZE seconds: {daily_optimize_seconds:.2f}")
print(f"Hourly patterns OPTIMIZE seconds: {hourly_optimize_seconds:.2f}")
print()

print_comparison(
    SILVER_CLEAN_TABLE,
    silver_before_metrics,
    silver_before_bench,
    silver_after_metrics,
    silver_after_bench,
)
print_comparison(
    GOLD_FCT_TRIPS_TABLE,
    gold_before_metrics,
    gold_before_bench,
    gold_after_metrics,
    gold_after_bench,
)
print_comparison(
    GOLD_DAILY_METRICS_TABLE,
    daily_before_metrics,
    None,
    daily_after_metrics,
    None,
)
print_comparison(
    GOLD_HOURLY_PATTERNS_TABLE,
    hourly_before_metrics,
    None,
    hourly_after_metrics,
    None,
)

print("=== Silver EXPLAIN FORMATTED before ===")
print(silver_before_explain)
print()
print("=== Silver EXPLAIN FORMATTED after ===")
print(silver_after_explain)
print()
print("=== Gold EXPLAIN FORMATTED before ===")
print(gold_before_explain)
print()
print("=== Gold EXPLAIN FORMATTED after ===")
print(gold_after_explain)
