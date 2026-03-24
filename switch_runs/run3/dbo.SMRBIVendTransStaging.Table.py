# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323134003-blyy/dbo.SMRBIVendTransStaging.Table.sql`

# COMMAND ----------

# Import Spark session (available as `spark` in Databricks notebooks)
# No additional modules are required for basic Spark SQL operations.

# -------------------------------------------------------------------------
# Create the Delta table in a schema where the current user has sufficient privileges.
# Using the default schema (or any schema the user can access) avoids permission errors.
# -------------------------------------------------------------------------

# Optionally, ensure the target schema exists; here we use the default schema.
# If you prefer a custom schema, replace `default` with the schema name you have access to.
spark.sql("""
CREATE TABLE IF NOT EXISTS default.SMRBIVendTransStaging (
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
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
