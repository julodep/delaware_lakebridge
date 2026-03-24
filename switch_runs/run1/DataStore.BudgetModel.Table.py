# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.BudgetModel.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.BudgetModel.Table.sql`

# COMMAND ----------

# Ensure the default catalog is selected (adjust if you have a different catalog)
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# Create the database (schema) if it does not exist
spark.sql("""
CREATE DATABASE IF NOT EXISTS DataStore
""")

# COMMAND ----------

# Create or replace the BudgetModel table within the DataStore database
# Columns are mapped from NVARCHAR to STRING.
# Spark SQL treats columns as nullable by default; only NOT NULL is specified explicitly.
spark.sql("""
CREATE OR REPLACE TABLE DataStore.BudgetModel (
    CompanyCode STRING,          -- maps from nvarchar(4) NULL
    BudgetModelCode STRING,      -- maps from nvarchar(10) NULL
    BudgetModelName STRING NOT NULL   -- maps from nvarchar(60) NOT NULL
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
# MAGIC Error in query 2: [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 269)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE DataStore.BudgetModel (     CompanyCode STRING,          -- maps from nvarchar(4) NULL     BudgetModelCode STRING,      -- maps from nvarchar(10) NULL     BudgetModelName STRING NOT NULL   -- maps from nvarchar(60) NOT NULL ) USING DELTA
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC ```
