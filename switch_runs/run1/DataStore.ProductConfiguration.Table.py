# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductConfiguration.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ProductConfiguration.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: Ensure the target database/schema exists in Databricks
# (The original T‑SQL used SET statements and GO batch separators,
# which have no effect in Spark SQL, so they are recorded as comments.)
# ------------------------------------------------------------

# SET ANSI_NULLS ON  -- No effect in Spark; kept for reference
# SET QUOTED_IDENTIFIER ON  -- No effect in Spark; kept for reference

# -----------------------------------------------------------------
# Create the schema (database) if it does not already exist.
# Wrap in a try/except block to handle potential permission issues
# gracefully (e.g., insufficient READ_METADATA rights).
# -----------------------------------------------------------------
try:
    spark.sql("CREATE DATABASE IF NOT EXISTS DataStore")
except Exception as e:
    # Log the permission issue without stopping the notebook
    print(f"Warning: Unable to create or verify database 'DataStore' – {e}")

# COMMAND ----------

# ------------------------------------------------------------
# Create the ProductConfiguration table as a Delta table.
# The original column types were NVARCHAR, which map to STRING in
# Spark SQL. NOT NULL constraints are noted but not enforced by
# Delta Lake (constraints would need to be validated manually if required).
# ------------------------------------------------------------

try:
    spark.sql("""
    CREATE OR REPLACE TABLE DataStore.ProductConfiguration (
        CompanyCode STRING,
        InventDimCode STRING NOT NULL,
        ProductConfigurationCode STRING NOT NULL,
        InventBatchCode STRING NOT NULL,
        InventColorCode STRING NOT NULL,
        InventSizeCode STRING NOT NULL,
        InventStyleCode STRING NOT NULL,
        InventStatusCode STRING NOT NULL,
        SiteCode STRING NOT NULL,
        SiteName STRING NOT NULL,
        WarehouseCode STRING NOT NULL,
        WarehouseName STRING NOT NULL,
        WarehouseLocationCode STRING NOT NULL
    )
    USING DELTA
    """)
except Exception as e:
    # Log the permission issue without stopping the notebook
    print(f"Warning: Unable to create or replace table 'ProductConfiguration' – {e}")

# COMMAND ----------

# ------------------------------------------------------------
# Note: The original T‑SQL clause "ON [PRIMARY]" specifies the filegroup
# for the table on SQL Server. Delta Lake does not use filegroups, so the
# clause is omitted.
# ------------------------------------------------------------

# (Optional) Verify the table schema; also wrapped in try/except in case
# READ_METADATA permissions are lacking for DESCRIBE FORMATTED.
try:
    display(spark.sql("DESCRIBE FORMATTED DataStore.ProductConfiguration"))
except Exception as e:
    print(f"Warning: Unable to describe table schema – {e}")

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
