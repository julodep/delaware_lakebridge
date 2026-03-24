# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Markup.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Markup.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: Create the target database if it does not already exist
# ------------------------------------------------------------
# Use the default catalog (spark_catalog) which all users have
# permission to read metadata from. Adjust the catalog name if
# your environment uses a different one.
spark.sql("""
CREATE DATABASE IF NOT EXISTS spark_catalog.DataStore
""")

# COMMAND ----------

# ------------------------------------------------------------
# NOTE: The original T‑SQL script contains session settings
#   SET ANSI_NULLS ON
#   SET QUOTED_IDENTIFIER ON
# These have no effect in Databricks/Spark and are therefore
# omitted (commented out for reference).
# ------------------------------------------------------------
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ------------------------------------------------------------
# Create the Markup table in the DataStore schema.
# The column definitions are converted to Spark SQL data types:
#   nvarchar(4)        -> STRING
#   bigint             -> LONG
#   int                -> INT
#   numeric(p,s)       -> DECIMAL(p,s)
# NULL / NOT NULL semantics are preserved.
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS spark_catalog.DataStore.Markup (
    CompanyCode STRING,
    TransRecId LONG NOT NULL,
    MarkupCategory INT NOT NULL,
    TransTableCode INT NOT NULL,
    SurchargeTransport DECIMAL(32,6) NOT NULL,
    SurchargePurchase DECIMAL(32,6) NOT NULL,
    SurchargeDelivery DECIMAL(32,6) NOT NULL,
    SurchargeTotal DECIMAL(34,6)
)
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
