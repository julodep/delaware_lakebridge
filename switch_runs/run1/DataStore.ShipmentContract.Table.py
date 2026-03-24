# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ShipmentContract.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ShipmentContract.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: Ensure the DataStore schema exists and create the ShipmentContract table
# ------------------------------------------------------------

# Create the database (schema) if it does not already exist.
# This grants the current user the required privileges to read metadata and use the schema.
spark.sql("""
CREATE DATABASE IF NOT EXISTS `DataStore`
""")

# COMMAND ----------

# Switch context to the DataStore schema.
spark.sql("USE `DataStore`")

# COMMAND ----------

# Create the Delta table with the required columns.
# NVARCHAR maps to STRING in Spark SQL, and BIGINT maps to BIGINT.
spark.sql("""
CREATE TABLE IF NOT EXISTS `ShipmentContract` (
    ShipmentContractId BIGINT NOT NULL,
    ShipmentContractCode STRING NOT NULL,
    ShipmentContractName STRING NOT NULL,
    ShipmentContractCodeName STRING NOT NULL,
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
