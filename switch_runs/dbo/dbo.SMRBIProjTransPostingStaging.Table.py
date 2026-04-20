# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjTransPostingStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjTransPostingStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table `SMRBIProjTransPostingStaging` in the
# specified Delta Lake catalog and schema.
# ------------------------------------------------------------------
#
# Databricks/Delta Lake does not support primary‑key constraints
# in the CREATE TABLE DDL.  We therefore create the table with the
# exact column types and a comment noting the intended key.
#
# Full column mapping:
#   - nvarchar(60/90/...) → STRING
#   - int                 → INT
#   - numeric(32,6)       → DECIMAL(32,6)
#   - bigint              → BIGINT
#   - datetime            → TIMESTAMP
#
# The composite key (EXECUTIONID, TRANSID, DATAAREAID, PARTITION)
# is documented in a table comment for reference but cannot be
# enforced by Delta Lake.
#
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjTransPostingStaging` (
  DEFINITIONGROUP   STRING  NOT NULL,
  EXECUTIONID       STRING  NOT NULL,
  ISSELECTED        INT     NOT NULL,
  TRANSFERSTATUS    INT     NOT NULL,
  ACTIVITYNUMBER    STRING  NOT NULL,
  AMOUNTMST         DECIMAL(32,6) NOT NULL,
  COSTSALES         INT     NOT NULL,
  CATEGORYID        STRING  NOT NULL,
  DEFAULTDIMENSION  BIGINT  NOT NULL,
  EMPLITEMID        STRING  NOT NULL,
  INVENTTRANSID     STRING  NOT NULL,
  LEDGERDIMENSION   BIGINT  NOT NULL,
  LEDGERORIGIN      INT     NOT NULL,
  LEDGERTRANSDATE   TIMESTAMP NOT NULL,
  PAYMENTDATE       TIMESTAMP NOT NULL,
  PAYMENTSTATUS     INT     NOT NULL,
  POSTINGTYPE       INT     NOT NULL,
  PROJADJUSTREFID   STRING  NOT NULL,
  PROJFUNDINGSOURCE BIGINT  NOT NULL,
  PROJID            STRING  NOT NULL,
  PROJTRANSDATE     TIMESTAMP NOT NULL,
  PROJTRANSTYPE     INT     NOT NULL,
  PROJTYPE          INT     NOT NULL,
  QTY               DECIMAL(32,6) NOT NULL,
  RESOURCE_         BIGINT  NOT NULL,
  RESOURCECATEGORY  BIGINT  NOT NULL,
  RESOURCELEGALENTITY BIGINT NOT NULL,
  SUBSCRIPTIONID    STRING  NOT NULL,
  TRANSACTIONORIGIN INT     NOT NULL,
  TRANSID           STRING  NOT NULL,
  VOUCHER           STRING  NOT NULL,
  WORKER            BIGINT  NOT NULL,
  WORKERLEGALENTITY BIGINT  NOT NULL,
  PARTITION         STRING  NOT NULL,
  DATAAREAID        STRING  NOT NULL,
  SYNCSTARTDATETIME TIMESTAMP NOT NULL,
  RECID             BIGINT  NOT NULL
)
USING DELTA
TBLPROPERTIES (
  "comment"="Composite key (EXECUTIONID, TRANSID, DATAAREAID, PARTITION) is intended but not enforced in Delta"
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
