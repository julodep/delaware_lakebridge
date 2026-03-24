# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Location.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Location.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Setup: import any required modules (Databricks provides Spark session)
# ------------------------------------------------------------------
# No additional imports are required for basic Spark SQL operations.

# ------------------------------------------------------------------
# NOTE: The following T‑SQL session settings are not applicable in Databricks.
# They are included as comments for reference.
# ------------------------------------------------------------------
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ------------------------------------------------------------------
# Ensure we are using a database where the current user has sufficient privileges.
# If a custom database is needed, it must be created first and proper permissions granted.
# Here we default to the built‑in `default` database.
# ------------------------------------------------------------------
spark.sql("USE default")

# COMMAND ----------

# ------------------------------------------------------------------
# Create the Delta table `Location` in the `default` database.
# Mapped data types:
#   - BIGINT  -> BIGINT (Spark SQL)
#   - NVARCHAR -> STRING
# ------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS Location (
    LocationId BIGINT,
    LocationCode STRING,
    LocationName STRING,
    LocationCodeName STRING,
    DimensionName STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
