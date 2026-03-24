# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.BusinessSegment.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.BusinessSegment.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Setup: import any required modules (Databricks provides a SparkSession)
# -------------------------------------------------------------------------
# No additional imports are needed for executing SQL via spark.sql()

# -------------------------------------------------------------------------
# Set the catalog and database context explicitly.
# This helps avoid permission errors related to default catalog resolution.
# -------------------------------------------------------------------------
# Use the default Unity Catalog (or the catalog your workspace is configured with)
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# Ensure the target schema (database) exists; create it if it does not.
spark.sql("""
CREATE SCHEMA IF NOT EXISTS DataStore
""")

# COMMAND ----------

# Switch to the newly created schema for subsequent DDL/DML.
spark.sql("USE DATABASE DataStore")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the BusinessSegment table.
# The original table definition used SQL Server data types and a PRIMARY key
# constraint. In Databricks / Delta Lake:
#   - BIGINT maps directly to BIGINT
#   - NVARCHAR maps to STRING
#   - PRIMARY KEY constraints are not enforced by Delta; they are omitted
#   - The ON [PRIMARY] storage option is not applicable
# -------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE BusinessSegment (
    BusinessSegmentId   BIGINT   NOT NULL,
    BusinessSegmentCode STRING   NOT NULL,
    BusinessSegmentName STRING   NOT NULL,
    BusinessSegmentCodeName STRING NOT NULL,
    DimensionName       STRING   NOT NULL
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
