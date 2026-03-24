# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Operations.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Operations.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Ensure the target database exists before creating the table.
# Using backticks around identifiers avoids case‑sensitivity issues
# and aligns with Spark SQL syntax.
# ------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS `DataStore`
COMMENT 'Database for storing operational data'
LOCATION 'dbfs:/user/hive/warehouse/datastore.db'
""")

# COMMAND ----------

# ------------------------------------------------------------
# Create the Operations table as a Delta table.
# Replaced the non‑Spark type LONG with BIGINT.
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS `DataStore`.`Operations` (
    OperationCode STRING NOT NULL,
    OperationName STRING NOT NULL,
    OperationSequence BIGINT,
    OperationNumber STRING NOT NULL,
    OperationNumberNext INT NOT NULL,
    CompanyCode STRING NOT NULL,
    OperationPriority INT NOT NULL
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
# MAGIC User does not have permission READ_METADATA on CATALOG.
# MAGIC User does not have permission READ_METADATA on any file. SQLSTATE: 42501
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
