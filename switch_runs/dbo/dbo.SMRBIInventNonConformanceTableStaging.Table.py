# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventNonConformanceTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventNonConformanceTableStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
# Create the staging table `SMRBIInventNonConformanceTableStaging`
# in the target catalog and schema.  The original T‑SQL table defined a
# composite primary key and several indexed/clustered options that are
# not supported by Delta Lake.  Those constraints are omitted – they can
# be re‑implemented by downstream data‑quality checks if required.
#
# All identifiers are written in fully‑qualified form:
#   `dbe_dbx_internships`.`dbo`.SMRBIInventNonConformanceTableStaging
#
# Data‑type mapping:
#   NVARCHAR          -> STRING
#   DATETIME          -> TIMESTAMP
#   NUMERIC(p, s)     -> DECIMAL(p, s)
#   INT              -> INT
# --------------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIInventNonConformanceTableStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CUSTACCOUNT STRING NOT NULL,
    INVENTDIMID STRING NOT NULL,
    INVENTNONCONFORMANCEAPPROVAL INT NOT NULL,
    INVENTNONCONFORMANCEID STRING NOT NULL,
    INVENTNONCONFORMANCETYPE INT NOT NULL,
    INVENTREFID STRING NOT NULL,
    INVENTTESTPROBLEMTYPEID STRING NOT NULL,
    INVENTTESTQUARANTINETYPE INT NOT NULL,
    INVENTTRANSIDREF STRING NOT NULL,
    INVENTTRANSTYPE INT NOT NULL,
    ITEMID STRING NOT NULL,
    NONCONFORMANCEDATE TIMESTAMP NOT NULL,
    RUSH INT NOT NULL,
    UNITID STRING NOT NULL,
    VENDACCOUNT STRING NOT NULL,
    HCMWORKER_PERSONNELNUMBER STRING NOT NULL,
    DEFAULTDIMENSIONDISPLAYVALUE STRING NOT NULL,
    COMPANY STRING NOT NULL,
    TESTDEFECTQTY DECIMAL(32,6) NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
) USING DELTA;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
