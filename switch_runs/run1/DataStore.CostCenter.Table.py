# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.CostCenter.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.CostCenter.Table.sql`

# COMMAND ----------

# Ensure the database (schema) exists before creating the table.
spark.sql("""
CREATE DATABASE IF NOT EXISTS DataStore
""")

# COMMAND ----------

# Create the CostCenter table within the DataStore database using Delta format.
spark.sql("""
CREATE TABLE IF NOT EXISTS DataStore.CostCenter (
    CostCenterId BIGINT NOT NULL,
    CostCenterCode STRING NOT NULL,
    CostCenterName STRING NOT NULL,
    CostCenterCodeName STRING NOT NULL,
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
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
