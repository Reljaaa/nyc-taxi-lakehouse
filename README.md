#nyc-taxi-lakehouse

Personal project — building a batch lakehouse on Azure Databricks with NYC Yellow Taxi data. Using Medallion architecture (bronze, silver, gold layers) with Delta Lake on ADLS Gen2.

Currently in active development.

## Stack

- Azure Databricks + ADLS Gen2
- PySpark, Spark SQL
- Delta Lake
- Databricks Workflows

## What I want to cover

- Idempotent ingestion with proper logging and DQ checks
- Star schema in the gold layer
- Some performance tuning (partitioning, Z-ORDER)

More docs to come as I make progress.
