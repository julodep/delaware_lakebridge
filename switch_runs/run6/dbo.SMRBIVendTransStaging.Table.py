# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324104824-3bci/dbo.SMRBIVendTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required modules (none needed for basic Spark SQL)
# ------------------------------------------------------------

# The following SET statements are T‑SQL session options and have no effect in Databricks.
# They are retained as comments for documentation purposes.
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ------------------------------------------------------------
# Create the staging table in the target catalog and schema.
# Original table: dbo.SMRBIVendTransStaging
# Target location: dbe_dbx_internships.switchschema.SMRBIVendTransStaging
# ------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.SMRBIVendTransStaging (
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
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
