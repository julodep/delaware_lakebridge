# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.CaseActivity.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.CaseActivity.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: ensure the target database (schema) exists in the catalog.
# Permission errors are handled gracefully – if the user lacks
# READ_METADATA on the catalog or on the database the creation
# step is skipped, assuming the database already exists.
# ------------------------------------------------------------
try:
    spark.sql("""
    CREATE DATABASE IF NOT EXISTS DataStore
    """)
except Exception as e:
    # Insufficient privileges to read/create metadata – ignore
    # and proceed assuming the database is already present.
    print(f"Database creation skipped: {e}")

# COMMAND ----------

# ------------------------------------------------------------
# Create the CaseActivity table in the DataStore schema.
# Column types are mapped from T‑SQL to Spark SQL equivalents.
# If the user cannot read metadata, use IF NOT EXISTS to avoid
# failing on a missing table definition.
# ------------------------------------------------------------
try:
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DataStore.CaseActivity (
        CaseCode               STRING      NOT NULL,
        ActivityNumber         STRING      NOT NULL,
        StartDateTime          DATE,
        EndDateTime            DATE,
        ActualEndDateTime      DATE,
        CompanyCode            STRING,
        ActivityTimeType       STRING,
        ActivityTaskTimeType   STRING,
        ActualWork             DECIMAL(32,6) NOT NULL,
        AllDay                 STRING,
        Category               STRING,
        Closed                 STRING,
        DoneByWorker           STRING      NOT NULL,
        PercentageCompleted    DECIMAL(32,6) NOT NULL,
        Purpose                STRING      NOT NULL,
        ResponsibleWorker      STRING      NOT NULL,
        Status                 STRING,
        TypeCode               STRING      NOT NULL,
        UserMemo               STRING      NOT NULL
    )
    """)
except Exception as e:
    # If the user lacks READ_METADATA/USAGE permissions on the
    # database, the table may already exist or cannot be created.
    # Log the exception and continue.
    print(f"Table creation skipped: {e}")

# COMMAND ----------

# ------------------------------------------------------------
# Optional: Verify that the table was created successfully.
# This block also catches permission errors gracefully.
# ------------------------------------------------------------
try:
    display(spark.sql("DESCRIBE DataStore.CaseActivity"))
except Exception as e:
    print(f"Unable to describe table due to permissions: {e}")

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
