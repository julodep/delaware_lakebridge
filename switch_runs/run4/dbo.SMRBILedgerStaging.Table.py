# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBILedgerStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324081459-mgpq/dbo.SMRBILedgerStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Create a dedicated database (if it does not already exist) to avoid
# permission issues on the default database.
# -------------------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS user_ledger_db
""")

# COMMAND ----------

# Switch the session to the newly created database.
spark.sql("USE user_ledger_db")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the SMRBILedgerStaging Delta table inside the user_ledger_db database.
# T‑SQL data types are mapped to Spark SQL types as follows:
#   nvarchar  -> STRING
#   int       -> INT
#   bigint    -> LONG
#   datetime  -> TIMESTAMP
#
# Primary key / index definitions are not supported in Delta Lake and are
# therefore omitted. If uniqueness is required, enforce it via Delta
# constraints or downstream logic.
# -------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS SMRBILedgerStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    BUDGETEXCHANGERATETYPE STRING NOT NULL,
    ACCOUNTINGCURRENCY STRING NOT NULL,
    CHARTOFACCOUNTSRECID LONG NOT NULL,
    EXCHANGERATETYPE STRING NOT NULL,
    NAME STRING NOT NULL,
    REPORTINGCURRENCY STRING NOT NULL,
    LEDGERRECID LONG NOT NULL,
    `PARTITION` STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
) USING DELTA
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
