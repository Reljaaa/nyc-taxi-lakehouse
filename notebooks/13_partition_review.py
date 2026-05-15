# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Partition Review
# MAGIC
# MAGIC One-time analysis notebook. **Run the single code cell below end-to-end.**
# MAGIC All setup, helpers, DESCRIBE DETAIL calls, EXPLAIN queries, and the final
# MAGIC report are consolidated into one cell to avoid Databricks serverless
# MAGIC cross-cell Python state loss.

# COMMAND ----------

# This is intentionally one large cell. Do NOT split it into multiple cells —
# Databricks serverless drops Python state between cells intermittently, which
# turns cross-cell function or constant references into runtime NameErrors.

import logging

from src.utils.logging import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

# ---------- Constants ----------

SILVER_CLEAN_TABLE     = "silver.default.yellow_trips_clean"
GOLD_FCT_TRIPS_TABLE   = "gold.default.fct_trips"
GOLD_DAILY_TABLE       = "gold.default.daily_metrics_by_zone"
GOLD_HOURLY_TABLE      = "gold.default.hourly_demand_patterns"
GOLD_DIM_ZONES_TABLE   = "gold.default.dim_zones"
GOLD_DIM_DATE_TABLE    = "gold.default.dim_date"
GOLD_DIM_PAYMENT_TABLE = "gold.default.dim_payment_type"
GOLD_DIM_RATE_TABLE    = "gold.default.dim_rate_code"
GOLD_DIM_VENDOR_TABLE  = "gold.default.dim_vendor"

ALL_TABLES = [
    SILVER_CLEAN_TABLE,
    GOLD_FCT_TRIPS_TABLE,
    GOLD_DAILY_TABLE,
    GOLD_HOURLY_TABLE,
    GOLD_DIM_ZONES_TABLE,
    GOLD_DIM_DATE_TABLE,
    GOLD_DIM_PAYMENT_TABLE,
    GOLD_DIM_RATE_TABLE,
    GOLD_DIM_VENDOR_TABLE,
]

# ---------- Helpers ----------

def describe_detail(table_fqn: str) -> dict:
    """Return key fields from DESCRIBE DETAIL for a Delta table."""
    logger.info("DESCRIBE DETAIL table=%s", table_fqn)
    row = spark.sql(f"DESCRIBE DETAIL {table_fqn}").first().asDict()
    return {
        "table": table_fqn,
        "numFiles": row.get("numFiles"),
        "sizeInBytes": row.get("sizeInBytes"),
        "partitionColumns": row.get("partitionColumns") or [],
    }


def explain_formatted(label: str, sql_text: str) -> str:
    """Return EXPLAIN FORMATTED output for a SQL query."""
    logger.info("EXPLAIN FORMATTED label=%s", label)
    rows = spark.sql(f"EXPLAIN FORMATTED {sql_text}").collect()
    return "\n".join(row[0] for row in rows)


def extract_partition_filter_line(explain_output: str) -> str:
    """Pull the PartitionFilters line from an EXPLAIN plan, or signal absence."""
    for line in explain_output.splitlines():
        if "PartitionFilters" in line:
            return line.strip()
    return "(no PartitionFilters — full partition scan)"


def print_detail(d: dict) -> None:
    """Print a compact DESCRIBE DETAIL summary."""
    size_mb = d["sizeInBytes"] / (1024 ** 2) if d["sizeInBytes"] else 0
    partitions = d["partitionColumns"] if d["partitionColumns"] else "none"
    print(
        f"  {d['table']:<50}  "
        f"numFiles={d['numFiles']:>5}  "
        f"size={size_mb:>8.1f} MB  "
        f"partitions={partitions}"
    )


# ---------- Section 1: DESCRIBE DETAIL — all tables ----------

print("=" * 70)
print("SECTION 1 — DESCRIBE DETAIL (partition columns + file stats)")
print("=" * 70)

details = []
for table in ALL_TABLES:
    d = describe_detail(table)
    details.append(d)
    print_detail(d)

print()

# ---------- Section 2: Partition pruning validation — gold.fct_trips ----------
#
# fct_trips is the only partitioned table (pickup_date_sk).
# We run three EXPLAIN queries:
#   A) Filter ON partition column  → expect PartitionFilters line
#   B) Filter OFF partition column → no PartitionFilters (full partition scan)
#   C) Filter on BOTH              → PartitionFilters + DictionaryFilters compose

FCT_WITH_DATE_FILTER = """
SELECT
    pickup_zone_sk,
    COUNT(*) AS trips,
    SUM(fare_amount) AS revenue
FROM gold.default.fct_trips
WHERE pickup_date_sk = 20240315
GROUP BY pickup_zone_sk
"""

FCT_WITHOUT_DATE_FILTER = """
SELECT
    pickup_zone_sk,
    COUNT(*) AS trips,
    SUM(fare_amount) AS revenue
FROM gold.default.fct_trips
WHERE pickup_zone_sk IN (1, 2, 3, 4)
GROUP BY pickup_zone_sk
"""

FCT_BOTH_FILTERS = """
SELECT
    pickup_zone_sk,
    COUNT(*) AS trips,
    SUM(fare_amount) AS revenue
FROM gold.default.fct_trips
WHERE pickup_date_sk = 20240315
  AND pickup_zone_sk IN (1, 2, 3, 4)
GROUP BY pickup_zone_sk
"""

explain_a = explain_formatted("fct_with_date_filter",    FCT_WITH_DATE_FILTER)
explain_b = explain_formatted("fct_without_date_filter", FCT_WITHOUT_DATE_FILTER)
explain_c = explain_formatted("fct_both_filters",        FCT_BOTH_FILTERS)

print("=" * 70)
print("SECTION 2 — Partition pruning validation (gold.fct_trips)")
print("=" * 70)
print()
print("A) Filter ON pickup_date_sk (partition column):")
print("   " + extract_partition_filter_line(explain_a))
print()
print("B) Filter only on pickup_zone_sk (non-partition column):")
print("   " + extract_partition_filter_line(explain_b))
print()
print("C) Filter on BOTH pickup_date_sk AND pickup_zone_sk:")
print("   " + extract_partition_filter_line(explain_c))
print()

# ---------- Section 3: Silver — confirm no partition pruning ----------

SILVER_ZONE_FILTER = """
SELECT
    pickup_location_id,
    COUNT(*) AS trips
FROM silver.default.yellow_trips_clean
WHERE pickup_location_id IN (132, 138, 161, 230)
GROUP BY pickup_location_id
"""

explain_silver = explain_formatted("silver_zone_filter", SILVER_ZONE_FILTER)

print("=" * 70)
print("SECTION 3 — Silver partition check (expect: no PartitionFilters)")
print("=" * 70)
print()
print("silver.yellow_trips_clean — filter on pickup_location_id:")
print("   " + extract_partition_filter_line(explain_silver))
print()

# ---------- Section 4: Full EXPLAIN plans (for reference) ----------

print("=" * 70)
print("SECTION 4 — Full EXPLAIN FORMATTED plans (for reference)")
print("=" * 70)
print()
print("--- A: fct_trips WHERE pickup_date_sk = 20240315 ---")
print(explain_a)
print()
print("--- B: fct_trips WHERE pickup_zone_sk IN (...) ---")
print(explain_b)
print()
print("--- C: fct_trips WHERE pickup_date_sk = ... AND pickup_zone_sk IN (...) ---")
print(explain_c)
print()
print("--- Silver WHERE pickup_location_id IN (...) ---")
print(explain_silver)
