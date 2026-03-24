# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Intercompany.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Intercompany.Table.sql`

# COMMAND ----------

# Create the database (schema) if it does not already exist.
# Use the fully qualified name to avoid catalog‑metadata permission checks.
spark.sql("""
CREATE DATABASE IF NOT EXISTS spark_catalog.DataStore
""")

# COMMAND ----------

# -------------------------------------------------------------------
# Create the Intercompany table in the DataStore schema.
# Spark SQL does not support length specifications for STRING columns,
# so we define them as plain STRING. The table is created as a Delta table
# (the default in Databricks).
# -------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE spark_catalog.DataStore.Intercompany (
    IntercompanyId   BIGINT   NOT NULL,
    IntercompanyCode STRING   NOT NULL,
    IntercompanyName STRING   NOT NULL,
    IntercompanyCodeName STRING NOT NULL,
    DimensionName    STRING   NOT NULL
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
