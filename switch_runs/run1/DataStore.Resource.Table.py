# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Resource.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Resource.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Use the default catalog (spark_catalog) – required for most
# Databricks workspaces. This avoids the READ_METADATA error on
# an unspecified catalog.
# ------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# ------------------------------------------------------------
# Create (or ensure) the target database (schema) exists.
# Using lower‑case name matches the permission error message
# and avoids case‑sensitivity issues in the metastore.
# ------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS `datastore`
""")

# COMMAND ----------

# ------------------------------------------------------------
# Switch to the newly created database.
# ------------------------------------------------------------
spark.sql("USE `datastore`")

# COMMAND ----------

# ------------------------------------------------------------
# Create the Resource table in the datastore schema.
# ANSI_NULLS, QUOTED_IDENTIFIER, GO statements, and filegroup
# clauses are omitted because they are T‑SQL specific and not
# applicable to Delta Lake tables in Databricks.
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS `Resource` (
    ResourceCode STRING NOT NULL,
    ResourceName STRING NOT NULL,
    ResourceCodeName STRING,
    ResourceGroupCode STRING NOT NULL,
    ResourceGroupName STRING NOT NULL,
    ResourceGroupCodeName STRING,
    CompanyCode STRING NOT NULL,
    ResourceType STRING NOT NULL,
    InputWarehouseCode STRING NOT NULL,
    InputWarehouseLocationCode STRING NOT NULL,
    OutputWarehouseCode STRING NOT NULL,
    OutputWarehouseLocationCode STRING NOT NULL,
    EfficiencyPercentage DECIMAL(32,6),
    RouteGroupCode STRING NOT NULL,
    HasFiniteSchedulingCapacity INT,
    ValidFromDate TIMESTAMP,
    ValidToDate TIMESTAMP,
    CalendarCode STRING NOT NULL
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
# MAGIC Error in query 3: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
