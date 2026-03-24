# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductionCapacity.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ProductionCapacity.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Setup: Ensure the target database exists and the user has appropriate access.
# -------------------------------------------------------------------------

# Create the database if it does not already exist.
# Using lowercase `datastore` to match typical Databricks naming conventions.
spark.sql("""
CREATE DATABASE IF NOT EXISTS datastore
""")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the ProductionCapacity table in the `datastore` schema.
# Column data types are mapped from T‑SQL to Spark SQL equivalents:
#   nvarchar   → STRING
#   datetime   → TIMESTAMP
#   numeric(p,s) → DECIMAL(p,s)
# -------------------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE datastore.ProductionCapacity (
    ProductionCapacityIdScreening STRING NOT NULL,
    CompanyCode                STRING NOT NULL,
    PlanVersion                STRING NOT NULL,
    CapacityDate               TIMESTAMP NOT NULL,
    CalendarCode               STRING NOT NULL,
    ResourceCode               STRING NOT NULL,
    RefType                    STRING NOT NULL,
    RefCode                    STRING NOT NULL,
    MaximumCapacity            DECIMAL(38,6),
    ReservedCapacity           DECIMAL(38,11),
    AvailableCapacity          DECIMAL(38,17)
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
