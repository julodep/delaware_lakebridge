# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBILedgerStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324104824-3bci/dbo.SMRBILedgerStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required modules (Databricks provides `spark` by default)
# ------------------------------------------------------------

# NOTE:
# - The original T‑SQL script contains batch separators (`GO`) and session settings
#   (`SET ANSI_NULLS ON`, `SET QUOTED_IDENTIFIER ON`). These have no effect in Databricks
#   and are therefore omitted.
# - The primary‑key definition, index options, and `ON [PRIMARY]` storage clauses are
#   not supported for Delta tables. The table will be created without an enforced PK,
#   but the column order that formed the PK in the source system is retained for reference.

# ------------------------------------------------------------
# Create the staging table in the target catalog and schema
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.SMRBILedgerStaging (
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
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------
# (Optional) Add a comment describing the original primary‑key intent.
# ------------------------------------------------------------
spark.sql("""
ALTER TABLE dbe_dbx_internships.switchschema.SMRBILedgerStaging
SET TBLPROPERTIES (
    'original_primary_key' = 'EXECUTIONID, LEDGERRECID, PARTITION'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
