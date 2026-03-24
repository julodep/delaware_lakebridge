# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324081459-mgpq/dbo.SMRBIVendTransStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Use the default catalog and schema (e.g., 'default') that the current user
# has permission to access. In Databricks the `dbo` schema may not be available
# or readable without additional grants.
# -------------------------------------------------------------------------

# Ensure we are using a schema the user can access
spark.sql("USE default")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the staging table in Delta Lake.
# Column data types are mapped from T‑SQL to Spark SQL equivalents:
#   nvarchar  -> STRING
#   int       -> INT
#   bigint    -> LONG
#   numeric   -> DECIMAL(p,s)
#   datetime  -> TIMESTAMP
# -------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS SMRBIVendTransStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ACCOUNTNUM STRING NOT NULL,
    AMOUNTCUR DECIMAL(32,6) NOT NULL,
    AMOUNTMST DECIMAL(32,6) NOT NULL,
    CLOSED TIMESTAMP NOT NULL,
    CURRENCYCODE STRING NOT NULL,
    DOCUMENTDATE TIMESTAMP NOT NULL,
    DUEDATE TIMESTAMP NOT NULL,
    INVOICE STRING NOT NULL,
    VENDTRANSRECID LONG NOT NULL,
    SETTLEAMOUNTCUR DECIMAL(32,6) NOT NULL,
    SETTLEAMOUNTMST DECIMAL(32,6) NOT NULL,
    TRANSDATE TIMESTAMP NOT NULL,
    TXT STRING NOT NULL,
    VOUCHER STRING NOT NULL,
    COMPANY STRING NOT NULL,
    APPROVED INT NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
