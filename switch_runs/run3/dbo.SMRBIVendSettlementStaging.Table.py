# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendSettlementStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323134003-blyy/dbo.SMRBIVendSettlementStaging.Table.sql`

# COMMAND ----------

# Create the staging table as a Delta table in the default schema.
# Use backticks for identifiers that contain special characters or match reserved words.
spark.sql("""
CREATE OR REPLACE TABLE `SMRBIVendSettlementStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ACCOUNTNUM STRING NOT NULL,
    EXCHADJUSTMENT DECIMAL(32,6) NOT NULL,
    SETTLEAMOUNTCUR DECIMAL(32,6) NOT NULL,
    SETTLEAMOUNTMST DECIMAL(32,6) NOT NULL,
    TRANSCOMPANY STRING NOT NULL,
    TRANSDATE TIMESTAMP NOT NULL,
    TRANSRECID BIGINT NOT NULL,
    VENDSETTLEMENTRECID BIGINT NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
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
