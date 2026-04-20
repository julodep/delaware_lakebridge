# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPdsRebateTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPdsRebateTableStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# Create a persistent Delta table in Unity Catalog.
#
# All references are fully‑qualified using the required `dbe_dbx_internships` and `dbo`
# placeholders.  The table definition mirrors the original T‑SQL:
#
# * NVARCHAR → STRING
# * INT      → INT
# * BIGINT   → BIGINT
# * NUMERIC(p,s) → DECIMAL(p,s)
# * DATETIME → TIMESTAMP
#
# The primary‑key constraint in T‑SQL cannot be enforced in Delta Lake,
# so it is omitted and a comment is added for clarity.
# --------------------------------------------------------------------

spark.sql("""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIPdsRebateTableStaging` (
    DEFINITIONGROUP              STRING NOT NULL,
    EXECUTIONID                  STRING NOT NULL,
    ISSELECTED                   INT NOT NULL,
    TRANSFERSTATUS               INT NOT NULL,
    REBATECREATEDDATETIME        TIMESTAMP NOT NULL,
    CURRENCYCODE                  STRING NOT NULL,
    CUSTACCOUNT                   STRING NOT NULL,
    CUSTINVOICETRANSREFRECID     BIGINT NOT NULL,
    COMPANY                       STRING NOT NULL,
    INVENTTRANSID                 STRING NOT NULL,
    ITEMID                        STRING NOT NULL,
    PDSCORRECTEDREBATEAMT         DECIMAL(32,6) NOT NULL,
    PDSPROCESSDATE              TIMESTAMP NOT NULL,
    PDSREBATEADJUSTMENTDATE     TIMESTAMP NOT NULL,
    PDSREBATEAMT                 DECIMAL(32,6) NOT NULL,
    PDSREBATEAMTTYPE              INT NOT NULL,
    PDSREBATEBALANCE             DECIMAL(32,6) NOT NULL,
    PDSREBATEID                   STRING NOT NULL,
    PDSREBATESTATUS               INT NOT NULL,
    PDSREBATETYPE                 STRING NOT NULL,
    PDSREBATEVALUE               DECIMAL(32,6) NOT NULL,
    REBATECALCDATE              TIMESTAMP NOT NULL,
    SALESAMOUNT                  DECIMAL(32,6) NOT NULL,
    SALESID                       STRING NOT NULL,
    SALESINVOICEDATE             TIMESTAMP NOT NULL,
    SALESINVOICEID                STRING NOT NULL,
    LEDGERDIMENSIONDISPLAYVALUE  STRING NOT NULL,
    PDSSTARTINGREBATEAMT          DECIMAL(32,6) NOT NULL,
    PDSREBATETABLEDIMENSION      BIGINT NOT NULL,
    PARTITION                     STRING NOT NULL,
    DATAAREAID                    STRING NOT NULL,
    SYNCSTARTDATETIME            TIMESTAMP NOT NULL
)
USING DELTA
COMMENT
    'Primary key constraint (EXECUTIONID, PDSREBATEID, DATAAREAID, PARTITION) is not enforced in Delta Lake; see Spark documentation for alternative consistency mechanisms if required.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
