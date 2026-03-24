# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Facility.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Facility.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Ensure we are using the default catalog (spark_catalog) 
# which most users have read metadata permissions on.
# ------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# ------------------------------------------------------------
# Create the database (schema) if it does not already exist.
# Use a consistent, lower‑case name to avoid case‑sensitivity 
# issues that can trigger permission errors.
# ------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS datastore
""")

# COMMAND ----------

# ------------------------------------------------------------
# Create the Facility table as a Delta table within the 
# `datastore` database. Spark SQL uses BIGINT for 64‑bit integers,
# which maps to the T‑SQL `bigint` type.
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS datastore.Facility (
    FacilityId       BIGINT   NOT NULL,
    FacilityCode     STRING   NOT NULL,
    FacilityName     STRING   NOT NULL,
    FacilityCodeName STRING   NOT NULL,
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
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
