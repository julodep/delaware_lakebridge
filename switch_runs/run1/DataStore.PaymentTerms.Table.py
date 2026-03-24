# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PaymentTerms.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.PaymentTerms.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: Ensure the target database/schema exists (if not already created)
# ------------------------------------------------------------
# In Unity Catalog environments the default catalog is `spark_catalog`.
# Use a fully qualified name to avoid permission issues on the generic CATALOG.
spark.sql("""
CREATE DATABASE IF NOT EXISTS `spark_catalog`.`DataStore`
""")

# COMMAND ----------

# ------------------------------------------------------------
# NOTE: The following T‑SQL session settings have no effect in Databricks
# and are therefore commented out. They are kept for reference.
# ------------------------------------------------------------
# SET ANSI_NULLS ON   -- Databricks always treats NULLs according to SQL standard
# SET QUOTED_IDENTIFIER ON   -- Quoted identifiers are always supported

# ------------------------------------------------------------
# Create the `PaymentTerms` table in the `DataStore` schema.
# The original T‑SQL uses `nvarchar` which maps to Spark `STRING`.
# NULL / NOT NULL constraints are preserved where possible.
# The `ON [PRIMARY]` storage clause is specific to SQL Server and is omitted.
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE `spark_catalog`.`DataStore`.`PaymentTerms` (
    CompanyCode STRING,
    PaymentTermsCode STRING,
    PaymentTermsName STRING NOT NULL
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
