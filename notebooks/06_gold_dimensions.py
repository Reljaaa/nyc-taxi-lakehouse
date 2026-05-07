# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Dimensions

# COMMAND ----------

import logging

from src.utils.logging import configure_logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


configure_logging()
logger = logging.getLogger(__name__)

GOLD_CATALOG = "gold"
GOLD_SCHEMA = "default"
LANDING_BASE_PATH = "abfss://landing@nyctaxilakehouse.dfs.core.windows.net"
ZONE_LOOKUP_PATH = f"{LANDING_BASE_PATH}/lookups/taxi_zone_lookup.csv"

DIM_ZONES_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.dim_zones"
DIM_DATE_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.dim_date"
DIM_PAYMENT_TYPE_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.dim_payment_type"
DIM_RATE_CODE_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.dim_rate_code"
DIM_VENDOR_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.dim_vendor"

DIM_KEY_MAP = {
    "dim_zones": (DIM_ZONES_TABLE, "location_id", "zone_sk"),
    "dim_date": (DIM_DATE_TABLE, "full_date", "date_sk"),
    "dim_payment_type": (
        DIM_PAYMENT_TYPE_TABLE,
        "payment_type_id",
        "payment_type_sk",
    ),
    "dim_rate_code": (DIM_RATE_CODE_TABLE, "rate_code_id", "rate_code_sk"),
    "dim_vendor": (DIM_VENDOR_TABLE, "vendor_id", "vendor_sk"),
}

DIMENSION_EXPECTED_COUNTS = {
    DIM_DATE_TABLE: 366,
    DIM_PAYMENT_TYPE_TABLE: 7,
    DIM_RATE_CODE_TABLE: 7,
    DIM_VENDOR_TABLE: 3,
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def create_gold_schema() -> None:
    """Create the target Gold schema if it does not already exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{GOLD_SCHEMA}")


def assert_unique_non_null_key(df: DataFrame, key_column: str, table_name: str) -> None:
    """Validate that the dimension surrogate key is non-null and unique."""
    total_count = df.count()
    null_count = df.filter(F.col(key_column).isNull()).count()
    distinct_count = df.select(key_column).distinct().count()

    assert null_count == 0, f"{table_name} has {null_count} null {key_column} values"
    assert distinct_count == total_count, (
        f"{table_name} {key_column} is not unique: "
        f"distinct={distinct_count}, total={total_count}"
    )


def assert_unique_natural_key(
    df: DataFrame,
    natural_key_column: str,
    table_name: str,
) -> None:
    """Validate that row_number ordering has no natural-key ties."""
    total_count = df.count()
    distinct_count = df.select(natural_key_column).distinct().count()
    assert distinct_count == total_count, (
        f"{table_name} natural key {natural_key_column} has duplicates: "
        f"distinct={distinct_count}, total={total_count}"
    )


def write_dimension(df: DataFrame, table_name: str, key_column: str) -> int:
    """Overwrite a Gold dimension table and validate its surrogate key."""
    logger.info("Writing Gold dimension table=%s", table_name)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    written_df = spark.table(table_name)
    assert_unique_non_null_key(written_df, key_column, table_name)
    row_count = written_df.count()
    logger.info("Gold dimension written table=%s row_count=%s", table_name, row_count)
    return row_count


def show_surrogate_key_sample(
    table_name: str,
    natural_key_column: str,
    surrogate_key_column: str,
) -> DataFrame:
    """Return a deterministic sample of natural-key to surrogate-key mappings."""
    return (
        spark.table(table_name)
        .select(natural_key_column, surrogate_key_column)
        .orderBy(natural_key_column)
        .limit(10)
    )


def collect_surrogate_key_sample(
    table_name: str,
    natural_key_column: str,
    surrogate_key_column: str,
) -> list[tuple[object, int]]:
    """Materialize a small deterministic surrogate-key sample for rerun comparison."""
    rows = show_surrogate_key_sample(
        table_name,
        natural_key_column,
        surrogate_key_column,
    ).collect()
    return [(row[natural_key_column], row[surrogate_key_column]) for row in rows]


def build_default_member_row(
    target_schema: StructType,
    surrogate_key_column: str,
    natural_key_column: str,
    description_column: str,
    description_value: str = "Not Provided",
) -> DataFrame:
    """Build the (sk=0, natural_key=0, description='Not Provided') default-member row.

    Reserved for fct_trips rows whose source natural key is NULL — keeps fact FKs non-null.
    """
    relaxed_schema = StructType(
        [StructField(f.name, f.dataType, nullable=True) for f in target_schema.fields]
    )
    row_values = []
    for field in target_schema.fields:
        if field.name == surrogate_key_column:
            row_values.append(0)
        elif field.name == natural_key_column:
            row_values.append(0)
        elif field.name == description_column:
            row_values.append(description_value)
        else:
            row_values.append(None)
    return spark.createDataFrame([tuple(row_values)], relaxed_schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build dim_zones

# COMMAND ----------

def read_zone_lookup_source() -> DataFrame:
    """Read the taxi zone lookup CSV from the landing zone."""
    return spark.read.option("header", True).csv(ZONE_LOOKUP_PATH).select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Zone").alias("zone_name"),
        F.col("Borough").alias("borough"),
        F.col("service_zone").alias("service_zone"),
    )


def build_dim_zones() -> DataFrame:
    """Build dim_zones from the taxi zone lookup CSV in landing."""
    zones_source_df = read_zone_lookup_source()
    assert_unique_natural_key(zones_source_df, "location_id", DIM_ZONES_TABLE)

    window_spec = Window.orderBy("location_id")
    real_members = zones_source_df.select(
        F.row_number().over(window_spec).cast("bigint").alias("zone_sk"),
        "location_id",
        "zone_name",
        "borough",
        "service_zone",
    )
    default_member = build_default_member_row(
        target_schema=real_members.schema,
        surrogate_key_column="zone_sk",
        natural_key_column="location_id",
        description_column="zone_name",
    )
    return default_member.unionByName(real_members)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build dim_date

# COMMAND ----------

def build_dim_date() -> DataFrame:
    """Build a full 2024 date dimension."""
    date_df = spark.sql(
        """
        SELECT explode(
            sequence(to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day)
        ) AS full_date
        """
    )

    return date_df.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_sk"),
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        # ISO day_of_week: 1=Monday, 7=Sunday.
        (F.weekday("full_date") + 1).alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekday("full_date").isin(5, 6).alias("is_weekend"),
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build hardcoded lookup dimensions

# COMMAND ----------

def add_deterministic_surrogate_key(
    df: DataFrame,
    natural_key_column: str,
    surrogate_key_column: str,
) -> DataFrame:
    """Add a deterministic BIGINT surrogate key ordered by a natural key."""
    window_spec = Window.orderBy(natural_key_column)
    return df.select(
        F.row_number().over(window_spec).cast("bigint").alias(surrogate_key_column),
        *df.columns,
    )


def build_dim_payment_type() -> DataFrame:
    """Build dim_payment_type from the approved static TLC lookup."""
    schema = StructType(
        [
            StructField("payment_type_id", IntegerType(), False),
            StructField("payment_type_description", StringType(), False),
        ]
    )
    df = spark.createDataFrame(
        [
            (1, "Credit"),
            (2, "Cash"),
            (3, "No charge"),
            (4, "Dispute"),
            (5, "Unknown"),
            (6, "Voided"),
        ],
        schema,
    )
    assert_unique_natural_key(df, "payment_type_id", DIM_PAYMENT_TYPE_TABLE)
    real_members = add_deterministic_surrogate_key(
        df,
        "payment_type_id",
        "payment_type_sk",
    )
    default_member = build_default_member_row(
        target_schema=real_members.schema,
        surrogate_key_column="payment_type_sk",
        natural_key_column="payment_type_id",
        description_column="payment_type_description",
    )
    return default_member.unionByName(real_members)


def build_dim_rate_code() -> DataFrame:
    """Build dim_rate_code from the approved static TLC lookup."""
    schema = StructType(
        [
            StructField("rate_code_id", IntegerType(), False),
            StructField("rate_code_description", StringType(), False),
        ]
    )
    df = spark.createDataFrame(
        [
            (1, "Standard rate"),
            (2, "JFK"),
            (3, "Newark"),
            (4, "Nassau/Westchester"),
            (5, "Negotiated fare"),
            (6, "Group ride"),
        ],
        schema,
    )
    assert_unique_natural_key(df, "rate_code_id", DIM_RATE_CODE_TABLE)
    real_members = add_deterministic_surrogate_key(df, "rate_code_id", "rate_code_sk")
    default_member = build_default_member_row(
        target_schema=real_members.schema,
        surrogate_key_column="rate_code_sk",
        natural_key_column="rate_code_id",
        description_column="rate_code_description",
    )
    return default_member.unionByName(real_members)


def build_dim_vendor() -> DataFrame:
    """Build dim_vendor from the approved static TLC lookup."""
    schema = StructType(
        [
            StructField("vendor_id", IntegerType(), False),
            StructField("vendor_name", StringType(), False),
        ]
    )
    df = spark.createDataFrame(
        [
            (1, "Creative Mobile Technologies"),
            (2, "VeriFone Inc"),
        ],
        schema,
    )
    assert_unique_natural_key(df, "vendor_id", DIM_VENDOR_TABLE)
    real_members = add_deterministic_surrogate_key(df, "vendor_id", "vendor_sk")
    default_member = build_default_member_row(
        target_schema=real_members.schema,
        surrogate_key_column="vendor_sk",
        natural_key_column="vendor_id",
        description_column="vendor_name",
    )
    return default_member.unionByName(real_members)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write and Validate Dimensions

# COMMAND ----------

def build_gold_dimensions() -> dict[str, int]:
    """Build, overwrite, and validate all Phase 4.2 Gold dimensions."""
    create_gold_schema()

    dimensions = [
        (DIM_ZONES_TABLE, build_dim_zones(), "zone_sk", None),
        (DIM_DATE_TABLE, build_dim_date(), "date_sk", DIMENSION_EXPECTED_COUNTS[DIM_DATE_TABLE]),
        (
            DIM_PAYMENT_TYPE_TABLE,
            build_dim_payment_type(),
            "payment_type_sk",
            DIMENSION_EXPECTED_COUNTS[DIM_PAYMENT_TYPE_TABLE],
        ),
        (
            DIM_RATE_CODE_TABLE,
            build_dim_rate_code(),
            "rate_code_sk",
            DIMENSION_EXPECTED_COUNTS[DIM_RATE_CODE_TABLE],
        ),
        (
            DIM_VENDOR_TABLE,
            build_dim_vendor(),
            "vendor_sk",
            DIMENSION_EXPECTED_COUNTS[DIM_VENDOR_TABLE],
        ),
    ]

    row_counts = {}
    for table_name, df, key_column, expected_count in dimensions:
        row_count = write_dimension(df, table_name, key_column)
        if expected_count is not None:
            assert row_count == expected_count, (
                f"{table_name} row count mismatch: "
                f"actual={row_count}, expected={expected_count}"
            )
        row_counts[table_name] = row_count

    zone_source_count = read_zone_lookup_source().select("location_id").distinct().count()
    expected_zone_count = zone_source_count + 1
    assert row_counts[DIM_ZONES_TABLE] == expected_zone_count, (
        f"{DIM_ZONES_TABLE} row count mismatch: "
        f"gold={row_counts[DIM_ZONES_TABLE]}, expected={expected_zone_count} "
        f"(zone_lookup_csv={zone_source_count} + 1 default member)"
    )
    logger.info("All Gold dimensions validated row_counts=%s", row_counts)
    return row_counts

# COMMAND ----------
# MAGIC %md
# MAGIC ## First Run

# COMMAND ----------

first_run_counts = build_gold_dimensions()
display(
    spark.createDataFrame(
        sorted(first_run_counts.items()),
        ["table_name", "row_count"],
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Idempotency Check

# COMMAND ----------

first_samples = {
    dimension_name: collect_surrogate_key_sample(
        table_name,
        natural_key_column,
        surrogate_key_column,
    )
    for dimension_name, (
        table_name,
        natural_key_column,
        surrogate_key_column,
    ) in DIM_KEY_MAP.items()
}

second_run_counts = build_gold_dimensions()
assert first_run_counts == second_run_counts, (
    f"Gold dimension row counts changed across rerun: "
    f"first={first_run_counts}, second={second_run_counts}"
)

second_samples = {}
for sample_name, first_sample in first_samples.items():
    table_name, natural_key_column, surrogate_key_column = DIM_KEY_MAP[sample_name]
    second_sample = collect_surrogate_key_sample(
        table_name,
        natural_key_column,
        surrogate_key_column,
    )
    second_samples[sample_name] = second_sample
    assert first_sample == second_sample, (
        f"{sample_name} surrogate-key sample changed across rerun: "
        f"first={first_sample}, second={second_sample}"
    )

display(
    spark.createDataFrame(
        sorted(second_run_counts.items()),
        ["table_name", "row_count_after_rerun"],
    )
)
display(
    spark.createDataFrame(
        [
            (sample_name, "first_run", str(natural_key), surrogate_key)
            for sample_name, sample_rows in first_samples.items()
            for natural_key, surrogate_key in sample_rows
        ]
        + [
            (sample_name, "second_run", str(natural_key), surrogate_key)
            for sample_name, sample_rows in second_samples.items()
            for natural_key, surrogate_key in sample_rows
        ],
        ["dimension", "run_label", "natural_key", "surrogate_key"],
    ).orderBy("dimension", "run_label", "natural_key")
)
display(spark.table(DIM_ZONES_TABLE).select("location_id", "zone_sk").orderBy("location_id").limit(10))
display(spark.table(DIM_DATE_TABLE).select("full_date", "date_sk").orderBy("full_date").limit(10))
display(
    spark.table(DIM_PAYMENT_TYPE_TABLE)
    .select("payment_type_id", "payment_type_sk")
    .orderBy("payment_type_id")
)
display(spark.table(DIM_RATE_CODE_TABLE).select("rate_code_id", "rate_code_sk").orderBy("rate_code_id"))
display(spark.table(DIM_VENDOR_TABLE).select("vendor_id", "vendor_sk").orderBy("vendor_id"))
