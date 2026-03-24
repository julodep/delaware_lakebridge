# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.BudgetCode.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.BudgetCode.Table.sql`

# COMMAND ----------

# Set the current catalog to the default Spark catalog.
# This avoids permission errors when the user lacks access to other catalogs.
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# Ensure the target database (schema) exists in Databricks.
# Use lower‑case name to match Spark's default handling of identifiers.
spark.sql("""
CREATE DATABASE IF NOT EXISTS datastore
""")

# COMMAND ----------

# Create the BudgetCode table using Delta Lake.
# Column types are mapped from T‑SQL to Spark SQL.
spark.sql("""
CREATE TABLE IF NOT EXISTS datastore.BudgetCode (
    CompanyCode STRING,
    BudgetTransactionCode BIGINT NOT NULL,
    BudgetCodeName STRING,
    BudgetCodeDescription STRING NOT NULL
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
