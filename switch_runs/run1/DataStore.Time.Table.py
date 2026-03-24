# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Time.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Time.Table.sql`

# COMMAND ----------

# Setup: import any required libraries (Databricks provides Spark session by default)
import sys  # Example import; add more if needed

# COMMAND ----------

# -------------------------------------------------------------------------
# Ensure we are using the correct catalog (default is spark_catalog)
# This avoids READ_METADATA permission errors on the default catalog.
# -------------------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the target database if it does not already exist.
# Fully qualify the database with the catalog to avoid permission look‑ups.
# -------------------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS spark_catalog.DataStore
""")

# COMMAND ----------

# -------------------------------------------------------------------------
# Switch context to the newly created database.
# -------------------------------------------------------------------------
spark.sql("USE DataStore")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the Time dimension table.
# Column types are mapped from T‑SQL to Spark SQL:
#   INT      -> INT
#   NVARCHAR -> STRING
# The ON [PRIMARY] clause has no equivalent in Delta Lake and is therefore
# omitted (commented for documentation).
# -------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS Time (
    DimTimeId   INT    NOT NULL,
    HourId      INT    NOT NULL,
    HourZoneCode STRING NOT NULL,
    HourZoneName STRING NOT NULL,
    MinuteId    INT    NOT NULL,
    Time        STRING NOT NULL
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
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 3: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
