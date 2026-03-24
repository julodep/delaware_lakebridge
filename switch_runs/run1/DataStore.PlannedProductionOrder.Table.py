# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PlannedProductionOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.PlannedProductionOrder.Table.sql`

# COMMAND ----------

# Ensure the target database (schema) exists in the Databricks metastore.
# Use the exact same case for the database name throughout the notebook
# to avoid permission and metadata lookup issues.
spark.sql("""
CREATE DATABASE IF NOT EXISTS `DataStore`
""")

# COMMAND ----------

# Switch to the newly created database so subsequent statements do not need
# to fully qualify the schema name.
spark.sql("USE `DataStore`")

# COMMAND ----------

# -------------------------------------------------------------------------
# Table creation – Delta format.
# The original script contained SET options and GO batch separators which
# are not applicable in Databricks. They are retained as comments for reference.
# -------------------------------------------------------------------------
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON
# GO

spark.sql("""
CREATE TABLE IF NOT EXISTS `PlannedProductionOrder` (
    PlannedProductionOrderCode STRING NOT NULL,
    ProductCode STRING NOT NULL,
    CompanyCode STRING NOT NULL,
    ProductConfigurationCode STRING NOT NULL,
    ProductionOrderCode STRING NOT NULL,
    RequirementDate TIMESTAMP NOT NULL,
    RequestedDate TIMESTAMP NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    DeliveryDate TIMESTAMP,
    Status STRING,
    LeadTime INT NOT NULL,
    InventoryUnit STRING NOT NULL,
    RequirementQuantity DECIMAL(32,6) NOT NULL,
    PurchaseUnit STRING NOT NULL,
    PurchaseQuantity DECIMAL(32,6) NOT NULL
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
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
