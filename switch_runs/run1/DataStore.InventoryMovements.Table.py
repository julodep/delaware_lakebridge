# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.InventoryMovements.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.InventoryMovements.Table.sql`

# COMMAND ----------

# Setup imports (Databricks provides Spark session by default)
import pyspark.sql.functions as F

# COMMAND ----------

# ------------------------------------------------------------------
# NOTE: The original T‑SQL script contains SET options and a GO batch separator.
# These are not applicable in Databricks Spark SQL, so they are commented out.
# ------------------------------------------------------------------
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ------------------------------------------------------------------
# Create the InventoryMovements table as a Delta table.
# Use a database where the current user has READ_METADATA and USAGE privileges.
# If you have a specific catalog/database you can replace `default` with that name.
# ------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE default.InventoryMovements (
    TransRecId        BIGINT,
    CompanyCode       STRING,
    Currency          STRING,
    CostPhysical      DECIMAL(38,6),
    CostFinancial     DECIMAL(38,6),
    CostAdjustment    DECIMAL(38,6)
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
