# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Create a Delta table that mirrors the original T‑SQL definition.
# All identifiers are fully‑qualified: `dbe_dbx_internships`.`dbo`.`SMRBIVendTransStaging`.
# ------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIVendTransStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID  STRING NOT NULL,
    ISSELECTED   INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ACCOUNTNUM   STRING NOT NULL,
    AMOUNTCUR    DECIMAL(32,6) NOT NULL,
    AMOUNTMST    DECIMAL(32,6) NOT NULL,
    CLOSED       TIMESTAMP NOT NULL,
    CURRENCYCODE STRING NOT NULL,
    DOCUMENTDATE TIMESTAMP NOT NULL,
    DUEDATE      TIMESTAMP NOT NULL,
    INVOICE      STRING NOT NULL,
    VENDTRANSRECID BIGINT NOT NULL,
    SETTLEAMOUNTCUR DECIMAL(32,6) NOT NULL,
    SETTLEAMOUNTMST DECIMAL(32,6) NOT NULL,
    TRANSDATE      TIMESTAMP NOT NULL,
    TXT            STRING NOT NULL,
    VOUCHER        STRING NOT NULL,
    COMPANY         STRING NOT NULL,
    APPROVED        INT NOT NULL,
    PARTITION       STRING NOT NULL,
    DATAAREAID      STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID          BIGINT NOT NULL
) USING DELTA
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
