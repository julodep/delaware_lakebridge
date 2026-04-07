# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendSettlementStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324104824-3bci/dbo.SMRBIVendSettlementStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required modules (Databricks provides spark by default)
# ------------------------------------------------------------
import sys  # noqa: F401

# COMMAND ----------

# ------------------------------------------------------------
# Note: The original T‑SQL script contains SET options and GO batch separators,
# which are not applicable in Databricks Spark SQL. They are omitted here.
# ------------------------------------------------------------

# ------------------------------------------------------------
# Create the Delta table SMRBIVendSettlementStaging in the target catalog
# and schema, mapping T‑SQL data types to Spark SQL equivalents.
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.SMRBIVendSettlementStaging (
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
    TRANSRECID LONG NOT NULL,
    VENDSETTLEMENTRECID LONG NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
