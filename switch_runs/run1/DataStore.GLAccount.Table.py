# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.GLAccount.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.GLAccount.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Setup: ensure the target database exists and create the GLAccount table
# -------------------------------------------------------------------------

# -------------------------------------------------------------------------
# 1. Set the catalog (required for metadata operations in Databricks)
# -------------------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")  # Adjust if a different catalog is needed

# COMMAND ----------

# -------------------------------------------------------------------------
# 2. Create the database (schema) if it does not already exist
# -------------------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS DataStore
""")

# COMMAND ----------

# -------------------------------------------------------------------------
# 3. Create the GLAccount table in the DataStore database
#    - Nullable columns are defined without an explicit NULL keyword (Spark SQL
#      treats columns as nullable by default).
#    - NOT NULL is kept where required.
# -------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE DataStore.GLAccount (
    CompanyCode STRING,
    GLAccountId LONG NOT NULL,
    GLAccountCode STRING NOT NULL,
    GLAccountName STRING NOT NULL,
    GLAccountType STRING,
    ChartOfAccountsName STRING NOT NULL,
    MainAccountCategory STRING NOT NULL,
    MainAccountCategoryDescription STRING NOT NULL,
    MainAccountCategoryCodeDescription STRING NOT NULL,
    MainAccountCategorySort INT NOT NULL,
    IsRevenueFlag BOOLEAN,
    IsGrossProfitFlag BOOLEAN
) USING DELTA
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
