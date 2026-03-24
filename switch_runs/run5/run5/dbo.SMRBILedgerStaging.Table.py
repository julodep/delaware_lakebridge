# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBILedgerStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324101927-oqdi/dbo.SMRBILedgerStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required libraries (Databricks provides Spark session by default)
# ------------------------------------------------------------
from pyspark.sql import functions as F

# COMMAND ----------

# ------------------------------------------------------------
# Create the SMRBILedgerStaging table in the target catalog and schema.
# The original T‑SQL creates a table with a clustered primary key and
# various index options that are not supported in Delta Lake.
# We therefore create a Delta table with equivalent column definitions,
# enforce NOT NULL where possible, and add a comment about the missing PK.
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.switchschema.SMRBILedgerStaging (
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
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
)
USING DELTA
COMMENT 'Created from T‑SQL script. Primary key and index options are omitted because Delta Lake does not enforce PK constraints or support the original clustering/statistics options.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
