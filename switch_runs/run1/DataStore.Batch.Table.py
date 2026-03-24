# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Batch.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Batch.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------------------
# Configuration: set the catalog name that the current user has
# access to. Replace 'my_catalog' with the appropriate catalog name.
# ----------------------------------------------------------------------
catalog_name = "my_catalog"  # <-- modify as needed

# COMMAND ----------

# Create the target database/schema if it does not already exist.
# Use a fully‑qualified name to avoid ambiguous catalog resolution.
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.DataStore")

# COMMAND ----------

# ----------------------------------------------------------------------
# NOTE:
# - Session options such as `SET ANSI_NULLS ON` and `SET QUOTED_IDENTIFIER ON`
#   are not applicable in Databricks/Spark SQL and have been omitted.
# - The `GO` batch separator is a T‑SQL construct and is not used here.
# - Filegroup options (e.g., `ON [PRIMARY]`) are specific to SQL Server and
#   are not supported by Delta Lake, so they are removed.
# - Column data types have been mapped to Spark SQL equivalents:
#       bigint  -> BIGINT
#       nvarchar -> STRING
#       datetime -> TIMESTAMP
# - `NOT NULL` constraints are kept where possible; Spark does not enforce
#   them strictly on Delta tables.
# ----------------------------------------------------------------------

# Create the Delta table `DataStore.Batch` with the mapped schema.
# The table name is fully‑qualified with the catalog to ensure the query
# runs against a catalog where the user has READ_METADATA/USAGE privileges.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.DataStore.Batch (
    RecId BIGINT,
    CompanyCode STRING,
    BatchCode STRING,
    ProductCode STRING NOT NULL,
    Description STRING NOT NULL,
    ExpiryDate TIMESTAMP NOT NULL,
    ProductionDate TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
