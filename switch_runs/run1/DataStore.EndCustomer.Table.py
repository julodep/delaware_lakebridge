# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.EndCustomer.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.EndCustomer.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required modules (Databricks provides Spark session)
# ------------------------------------------------------------
# No additional imports are needed for executing SQL via spark.sql()

# ------------------------------------------------------------
# Ensure a database the user has access to exists.
# Using the default database avoids permission issues with `DataStore`.
# ------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS default")

# COMMAND ----------

# ------------------------------------------------------------
# Create the Delta table `EndCustomer` in the accessible database.
# Column mappings: BIGINT → BIGINT, NVARCHAR → STRING.
# NOT NULL constraints are omitted (Delta does not enforce them by default).
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE default.EndCustomer (
    EndCustomerId BIGINT,
    EndCustomerCode STRING,
    EndCustomerName STRING,
    EndCustomerCodeName STRING,
    DimensionName STRING
)
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
