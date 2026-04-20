# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendPackingSlipJourStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendPackingSlipJourStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the SMRBIVendPackingSlipJourStaging table in the
# specified catalog and schema.  Spark SQL (Delta Lake) does not
# support primary‑key constraints, so we create the table without
# explicit keys and add a comment explaining the omission.
# --------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendPackingSlipJourStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    DELIVERYDATE TIMESTAMP NOT NULL,
    DELIVERYNAME STRING NOT NULL,
    DELIVERYTYPE INT NOT NULL,
    DLVMODE STRING NOT NULL,
    DLVTERM STRING NOT NULL,
    FREIGHTSLIPNUM STRING NOT NULL,
    FREIGHTSLIPTYPE INT NOT NULL,
    INTERCOMPANYCOMPANYID STRING NOT NULL,
    INTERCOMPANYSALESID STRING NOT NULL,
    INVOICEACCOUNT STRING NOT NULL,
    ORDERACCOUNT STRING NOT NULL,
    PACKINGSLIPID STRING NOT NULL,
    PURCHASETYPE INT NOT NULL,
    PURCHID STRING NOT NULL,
    VENDPACKINGSLIPJOURRECID LONG NOT NULL,
    COMPANY STRING NOT NULL,
    VENDPACKINGSLIPJOURDIMENSION LONG NOT NULL,
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
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
