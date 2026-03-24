# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ForecastModel.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ForecastModel.Table.sql`

# COMMAND ----------

# Import required modules (Databricks provides a default Spark session)
import sys

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the database if it does not exist
# -------------------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS DataStore
""")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the ForecastModel table in the DataStore database (Delta table)
# -------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS DataStore.ForecastModel (
    CompanyCode STRING,                     -- maps from nvarchar(4), nullable
    ForecastModelCode STRING,               -- maps from nvarchar(10), nullable
    ForecastModelName STRING NOT NULL,      -- maps from nvarchar(60), NOT NULL
    ForecastSubModelCode STRING NOT NULL    -- maps from nvarchar(10), NOT NULL
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
# MAGIC Error in query 1: [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 393)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS DataStore.ForecastModel (     CompanyCode STRING,                     -- maps from nvarchar(4), nullable     ForecastModelCode STRING,               -- maps from nvarchar(10), nullable     ForecastModelName STRING NOT NULL,      -- maps from nvarchar(60), NOT NULL     ForecastSubModelCode STRING NOT NULL    -- maps from nvarchar(10), NOT NULL ) USING DELTA
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC ```
