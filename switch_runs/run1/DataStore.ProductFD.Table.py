# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductFD.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ProductFD.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Setup: Create the DataStore.ProductFD table in Databricks (Delta Lake)
# ------------------------------------------------------------------

# Ensure the target database exists and that the current user has the required privileges.
# In Databricks the database name is case‑insensitive; we create it in lower‑case
# to match the permission check that raised the error.
spark.sql("""
CREATE DATABASE IF NOT EXISTS `datastore`
""")

# COMMAND ----------

# Optionally switch the session to the newly created database.
spark.sql("USE `datastore`")

# COMMAND ----------

# Create (or replace) the Delta table with the specified schema.
# Identifiers that require quoting are wrapped in backticks.
spark.sql("""
CREATE OR REPLACE TABLE `datastore`.`ProductFD` (
    ProductFDId       BIGINT   NOT NULL,
    ProductFDCode     STRING   NOT NULL,
    ProductFDName     STRING   NOT NULL,
    ProductFDCodeName STRING   NOT NULL,
    DimensionName    STRING   NOT NULL
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
