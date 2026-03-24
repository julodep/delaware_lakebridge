# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.NetRequirements.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.NetRequirements.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: Import any required libraries (Databricks provides `spark` by default)
# ------------------------------------------------------------

# No additional imports are necessary for creating a Delta table.

# ------------------------------------------------------------
# Ensure the target database exists and the user has proper access.
# ------------------------------------------------------------

spark.sql("""
CREATE DATABASE IF NOT EXISTS `DataStore`
""")

# COMMAND ----------

# ------------------------------------------------------------
# Create the Delta table `DataStore.NetRequirements`
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS `DataStore`.`NetRequirements` (
    RecId               BIGINT          NOT NULL,
    CompanyCode         STRING          NOT NULL,
    ReferenceType       STRING          NOT NULL,
    PlanVersion         STRING          NOT NULL,
    ProductCode         STRING          NOT NULL,
    InventDimCode       STRING          NOT NULL,
    RequirementDate     TIMESTAMP,
    RequirementTime     STRING          NOT NULL,
    RequirementDateTime TIMESTAMP,
    ReferenceCode       STRING          NOT NULL,
    ProducedItemCode    STRING,
    CustomerCode        STRING          NOT NULL,
    VendorCode          STRING          NOT NULL,
    ActionDate          TIMESTAMP,
    ActionDays          INT             NOT NULL,
    ActionType          STRING          NOT NULL,
    ActionMarked        STRING          NOT NULL,
    FuturesDate         TIMESTAMP       NOT NULL,
    FuturesDays         INT             NOT NULL,
    FuturesCalculated   STRING          NOT NULL,
    FuturesMarked       STRING          NOT NULL,
    Direction           STRING          NOT NULL,
    RankNr              BIGINT,
    Quantity            DECIMAL(32,6)   NOT NULL,
    QuantityConfirmed   DECIMAL(32,6)   NOT NULL
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
