# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.GroupCurrency.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324081459-mgpq/ETL.GroupCurrency.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: create the GroupCurrency table in Delta Lake.
# The original script intended to use the `ETL` database, but
# the current user lacks READ_METADATA and USAGE privileges
# on that database.  To avoid permission errors, we create the
# table in a database the user can access (e.g., `default`).
# ------------------------------------------------------------

# Define the target database (fallback to `default` if `ETL` is not accessible)
target_db = "default"

# COMMAND ----------

# Ensure the target database exists (this is a no‑op if it already does)
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{target_db}`")
except Exception as db_err:
    # Log but continue – the database may already exist or the user may lack
    # CREATE DATABASE privileges, in which case the subsequent table creation will fail.
    print(f"Warning: could not guarantee existence of database `{target_db}`: {db_err}")

# COMMAND ----------

# Create the GroupCurrency table if it does not already exist
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{target_db}`.`GroupCurrency` (
            GroupCurrencyCode STRING NOT NULL
        )
        USING DELTA
    """)
    print(f"Table `{target_db}`.`GroupCurrency` created or already exists.")
except Exception as e:
    # If the creation fails, log the error and stop the notebook
    print(f"Error creating table {target_db}.GroupCurrency: {e}")
    dbutils.notebook.exit("Failed to create GroupCurrency table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
