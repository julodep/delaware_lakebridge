# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjInvoiceOnAccStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjInvoiceOnAccStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
#  Create the staging table `SMRBIProjInvoiceOnAccStaging`
#  ================================================
#  *All identifiers are fully‑qualified with the target catalog
#   and schema (`dbe_dbx_internships`.`dbo`).*
#  *Map T‑SQL datatypes to Spark/Delta types:*
#   - NVARCHAR → STRING
#   - INT       → INT
#   - NUMERIC   → DECIMAL(p,s)
#   - DATETIME  → TIMESTAMP
#   - BIGINT    → BIGINT
#  *Primary key definition is not enforced in Delta Lake; a
#   comment explains this limitation.*
# -------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjInvoiceOnAccStaging` (
    DEFINITIONGROUP          STRING  NOT NULL,
    EXECUTIONID              STRING  NOT NULL,
    ISSELECTED               INT     NOT NULL,
    TRANSFERSTATUS           INT     NOT NULL,
    AMOUNT                   DECIMAL(32,6) NOT NULL,
    CURRENCYID               STRING  NOT NULL,
    COMPANY                  STRING  NOT NULL,
    INVOICEDATE              TIMESTAMP NOT NULL,
    PROJID                   STRING  NOT NULL,
    PROJINVOICEID            STRING  NOT NULL,
    QTY                      DECIMAL(32,6) NOT NULL,
    TAXAMOUNT                DECIMAL(32,6) NOT NULL,
    TRANSDATE                TIMESTAMP NOT NULL,
    TRANSID                  STRING  NOT NULL,
    TXT                      STRING  NOT NULL,
    PARTITION                STRING  NOT NULL,
    DATAAREAID               STRING  NOT NULL,
    SYNCSTARTDATETIME        TIMESTAMP NOT NULL,
    RECID                    BIGINT  NOT NULL
)
USING delta
LOCATION '/delta/dbe_dbx_internships/dbo/SMRBIProjInvoiceOnAccStaging';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
