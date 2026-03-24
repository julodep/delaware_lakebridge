# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductCostBreakdownHierarchy.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ProductCostBreakdownHierarchy.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: No additional imports required; Databricks provides `spark`
# ------------------------------------------------------------

# The following T‑SQL session settings are not applicable in Databricks.
# They are retained as comments for reference.
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# Create the Delta table `ProductCostBreakdownHierarchy` in the current catalog/database.
# If a specific database is required and the user has appropriate permissions,
# replace `default` with that database name.
spark.sql("""
CREATE OR REPLACE TABLE `default`.`ProductCostBreakdownHierarchy` (
    CompanyCode STRING,
    Level_1 STRING NOT NULL,
    Level_2 STRING NOT NULL,
    Level_3 STRING NOT NULL
)
USING DELTA
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
