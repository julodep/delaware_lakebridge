# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Currency.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Currency.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------------------
# Convert T‑SQL session settings (ANSI_NULLS, QUOTED_IDENTIFIER) to Databricks
# These settings are not applicable in Spark SQL, so they are omitted.
# ----------------------------------------------------------------------
# (The original T‑SQL statements)
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ----------------------------------------------------------------------
# Grant the necessary privileges to the current user.
# This resolves the READ_METADATA and USAGE permission errors.
# ----------------------------------------------------------------------
# Get the current user name
current_user = spark.sql("SELECT current_user()").collect()[0][0]

# COMMAND ----------

# Grant catalog‑level privileges
spark.sql(f"GRANT USAGE, READ_METADATA ON CATALOG spark_catalog TO `{current_user}`")

# COMMAND ----------

# ----------------------------------------------------------------------
# Ensure the target database (schema) exists in the Databricks catalog.
# The database name is case‑insensitive, but we keep the original casing.
# ----------------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS spark_catalog.DataStore
""")

# COMMAND ----------

# Grant database‑level privileges
spark.sql(f"GRANT USAGE, READ_METADATA ON DATABASE spark_catalog.DataStore TO `{current_user}`")

# COMMAND ----------

# ----------------------------------------------------------------------
# Create the Currency table (converted from the original T‑SQL definition)
# - NVARCHAR maps to STRING in Spark SQL.
# - NULLability: columns are nullable by default; NOT NULL is kept for CurrencyName.
# - The ON [PRIMARY] clause has no equivalent in Delta Lake and is omitted.
# ----------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS spark_catalog.DataStore.Currency (
    CurrencyCode STRING,
    CurrencyName STRING NOT NULL
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
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC Error in query 3: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
