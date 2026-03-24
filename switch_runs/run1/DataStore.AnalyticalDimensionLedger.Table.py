# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AnalyticalDimensionLedger.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.AnalyticalDimensionLedger.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: Databricks provides a SparkSession (`spark`) by default.
# The following T‑SQL session settings have no effect in Databricks,
# so they are documented as comments for reference.
# ------------------------------------------------------------

# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ------------------------------------------------------------
# Ensure we are using the default catalog that the current user
# has metadata read permissions for.  In Unity Catalog environments
# the default catalog is usually `spark_catalog`.
# ------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# ------------------------------------------------------------
# Create the target database (schema) if it does not already exist.
# Use a lowercase name to match the permissions reported in the
# error messages (datastore).
# ------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS datastore
""")

# COMMAND ----------

# ------------------------------------------------------------
# Create the AnalyticDimensionLedger table as a Delta table.
# Bracketed identifiers are replaced with backticks (if needed) and
# SQL Server data types are mapped to Spark SQL equivalents.
# BIGINT in T‑SQL maps to BIGINT in Spark SQL.
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS datastore.analyticaldimensionledger (
    LedgerDimensionId BIGINT NOT NULL,
    MainAccount       BIGINT NOT NULL,
    Intercompany      BIGINT NOT NULL,
    BusinessSegment   BIGINT NOT NULL,
    EndCustomer       BIGINT NOT NULL,
    Department        BIGINT NOT NULL,
    LocalAccount      BIGINT NOT NULL,
    Location          BIGINT NOT NULL,
    Product           BIGINT NOT NULL,
    ShipmentContract  BIGINT NOT NULL,
    Vendor            BIGINT NOT NULL
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
