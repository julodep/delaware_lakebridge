# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.AssetJournalV2EntityStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.AssetJournalV2EntityStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the AssetJournalV2EntityStaging table in the target
# catalog/schema as a Delta Lake table.  All column types have
# been translated from SQL Server to Spark SQL equivalents:
#
#   * NVARCHAR(n)      -> STRING
#   * INT              -> INT
#   * NUMERIC(p,s)     -> DECIMAL(p,s)
#   * DATETIME         -> TIMESTAMP
#
# NOTE:
#   * Delta Lake does not support PRIMARY KEY or UNIQUE
#     constraints natively.  The PK definition from the
#     T‑SQL statement is omitted.  If you require
#     uniqueness you’ll need to enforce it at the application
#     level or using a CONSTRAINT and then let a custom
#     enforcement process check it.
#   * Indexes (CLUSTERED, NONCLUSTERED) are ignored because
#     Delta Lake automatically partitions and sorts the data.
# --------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`AssetJournalV2EntityStaging`
(
    ACCOUNTDISPLAYVALUE STRING NOT NULL,
    OFFSETACCOUNTDISPLAYVALUE STRING NOT NULL,
    DEFAULTDIMENSIONDISPLAYVALUE STRING NOT NULL,
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CREDITAMOUNT DECIMAL(32,6) NOT NULL,
    DEBITAMOUNT DECIMAL(32,6) NOT NULL,
    JOURNALBATCHNUMBER STRING NOT NULL,
    OFFSETACCOUNTTYPE INT NOT NULL,
    TRANSDATE TIMESTAMP NOT NULL,
    TEXT STRING NOT NULL,
    VOUCHER STRING NOT NULL,
    CURRENCYCODE STRING NOT NULL,
    POSTINGLAYER INT NOT NULL,
    JOURNALNAMEID STRING NOT NULL,
    LINENUMBER DECIMAL(32,16) NOT NULL,
    ISPOSTED INT NOT NULL,
    DESCRIPTION STRING NOT NULL,
    BOOKID STRING NOT NULL,
    TRANSACTIONTYPE INT NOT NULL,
    ACCOUNTTYPE INT NOT NULL,
    CONSUMPTIONUNITS DECIMAL(32,6) NOT NULL,
    EXCHANGERATE DECIMAL(32,16) NOT NULL,
    CHINESEVOUCHERTYPE STRING NOT NULL,
    CHINESEVOUCHER STRING NOT NULL,
    CREDITAMOUNTREPORTINGCURRENCY DECIMAL(32,6) NOT NULL,
    DEBITAMOUNTREPORTINGCURRENCY DECIMAL(32,6) NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
