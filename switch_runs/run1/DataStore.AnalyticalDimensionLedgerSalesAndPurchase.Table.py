# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AnalyticalDimensionLedgerSalesAndPurchase.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.AnalyticalDimensionLedgerSalesAndPurchase.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: No special imports required; Databricks provides `spark` by default.
# ------------------------------------------------------------

# ------------------------------------------------------------
# Create the Analytic Dimension Ledger table in the default database.
# Note:
#   • Square brackets are replaced with backticks for identifiers.
#   • Data types are mapped to Spark SQL equivalents.
#   • Delta Lake is used as the storage format (the default for Databricks).
#   • Primary key / index definitions are not supported in Delta and are omitted.
# ------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE `default`.`AnalyticalDimensionLedgerSalesAndPurchase` (
    `DefaultDimensionId` BIGINT,
    `MainAccount` STRING,
    `Intercompany` STRING,
    `BusinessSegment` STRING,
    `EndCustomer` STRING,
    `Department` STRING,
    `LocalAccount` STRING,
    `Location` STRING,
    `Product` STRING,
    `ShipmentContract` STRING,
    `Vendor` STRING
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
