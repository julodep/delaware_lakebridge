# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324101927-oqdi/dbo.SMRBIVendTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Create the SMRBIVendTransStaging table in the target catalog/schema.
# NOTE:
# - T‑SQL `SET ANSI_NULLS ON` and `SET QUOTED_IDENTIFIER ON` have no effect in Databricks and are therefore omitted.
# - The `ON [PRIMARY]` storage clause is specific to SQL Server and is not supported; it is commented out.
# - Data types are mapped to Spark SQL equivalents (see guidelines).
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.switchschema.SMRBIVendTransStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT    NOT NULL,
    TRANSFERSTATUS INT   NOT NULL,
    ACCOUNTNUM    STRING NOT NULL,
    AMOUNTCUR     DECIMAL(32,6) NOT NULL,
    AMOUNTMST     DECIMAL(32,6) NOT NULL,
    CLOSED        TIMESTAMP NOT NULL,
    CURRENCYCODE  STRING NOT NULL,
    DOCUMENTDATE  TIMESTAMP NOT NULL,
    DUEDATE       TIMESTAMP NOT NULL,
    INVOICE       STRING NOT NULL,
    VENDTRANSRECID LONG   NOT NULL,
    SETTLEAMOUNTCUR DECIMAL(32,6) NOT NULL,
    SETTLEAMOUNTMST DECIMAL(32,6) NOT NULL,
    TRANSDATE     TIMESTAMP NOT NULL,
    TXT           STRING NOT NULL,
    VOUCHER       STRING NOT NULL,
    COMPANY       STRING NOT NULL,
    APPROVED      INT    NOT NULL,
    PARTITION     STRING NOT NULL,
    DATAAREAID    STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID         LONG   NOT NULL
)
""")  # No storage options like ON [PRIMARY] are applicable in Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
