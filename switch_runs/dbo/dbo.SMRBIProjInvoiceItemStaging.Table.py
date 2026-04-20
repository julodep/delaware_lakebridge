# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjInvoiceItemStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjInvoiceItemStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create a Delta Lake table for staging invoice items
# --------------------------------------------------------------
# The original T‑SQL statement used a primary‑key and some index options
# which have no direct equivalent in Databricks Delta Lake.
# We therefore create the table with the same columns, but omit
# primary‑key, statistics, and optimizer hints, and use backticks
# for the reserved column name PARTITION.
# --------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjInvoiceItemStaging` (
    DEFINITIONGROUP    STRING   NOT NULL,
    EXECUTIONID        STRING   NOT NULL,
    ISSELECTED         INT      NOT NULL,
    TRANSFERSTATUS     INT      NOT NULL,
    ACTIVITYNUMBER     STRING   NOT NULL,
    CATEGORYID         STRING   NOT NULL,
    CURRENCYID         STRING   NOT NULL,
    COMPANY            STRING   NOT NULL,
    INVOICEDATE        TIMESTAMP NOT NULL,
    ITEMID             STRING   NOT NULL,
    LINEAMOUNT         DECIMAL(32,6) NOT NULL,
    PROJID             STRING   NOT NULL,
    PROJINVOICEID      STRING   NOT NULL,
    PROJTRANSID        STRING   NOT NULL,
    QTY                DECIMAL(32,6) NOT NULL,
    TAXAMOUNT          DECIMAL(32,6) NOT NULL,
    TRANSDATE          TIMESTAMP NOT NULL,
    TXT                STRING   NOT NULL,
    `PARTITION`        STRING   NOT NULL,
    DATAAREAID         STRING   NOT NULL,
    SYNCSTARTDATETIME  TIMESTAMP NOT NULL,
    RECID              BIGINT   NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
