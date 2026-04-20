# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjInvoiceCostStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjInvoiceCostStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# 1. Create the staging table in dbe_dbx_internships.dbo
# -------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.SMRBIProjInvoiceCostStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ACTIVITYNUMBER STRING NOT NULL,
    CATEGORYID STRING NOT NULL,
    CURRENCYID STRING NOT NULL,
    COMPANY STRING NOT NULL,
    INVOICEDATE TIMESTAMP NOT NULL,
    LINEAMOUNT DECIMAL(32,6) NOT NULL,
    PROJID STRING NOT NULL,
    PROJINVOICEID STRING NOT NULL,
    QTY DECIMAL(32,6) NOT NULL,
    TAXAMOUNT DECIMAL(32,6) NOT NULL,
    TRANSDATE TIMESTAMP NOT NULL,
    TRANSID STRING NOT NULL,
    TXT STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# -------------------------------------------------------------
# 2. Verify that the table has been created correctly.
# -------------------------------------------------------------
spark.sql(f"SHOW COLUMNS IN `dbe_dbx_internships`.`dbo`.SMRBIProjInvoiceCostStaging").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
