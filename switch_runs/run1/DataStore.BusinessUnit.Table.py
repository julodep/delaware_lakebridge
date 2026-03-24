# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.BusinessUnit.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.BusinessUnit.Table.sql`

# COMMAND ----------

# Ensure the database exists before creating tables in it.
spark.sql("""
CREATE DATABASE IF NOT EXISTS DataStore
""")

# COMMAND ----------

# Switch to the DataStore database to avoid fully‑qualified names that may trigger permission checks.
spark.sql("USE DataStore")

# COMMAND ----------

# Create the BusinessUnit Delta table.
spark.sql("""
CREATE TABLE IF NOT EXISTS BusinessUnit (
    BusinessUnitId BIGINT NOT NULL,
    BusinessUnitCode STRING NOT NULL,
    BusinessUnitName STRING NOT NULL,
    BusinessUnitCodeName STRING NOT NULL,
    DimensionName STRING NOT NULL
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
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
