# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustInvoiceJourStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustInvoiceJourStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
#  Create the staging table `SMRBICustInvoiceJourStaging`
#
#  - Table is created in the target catalog and schema with the fully‑qualified
#    name: `dbe_dbx_internships`.`dbo`.`SMRBICustInvoiceJourStaging`
#  - Data types are mapped from T‑SQL to Spark SQL according to the
#    guidelines (e.g. VARCHAR -> STRING, NUMERIC(p,s) -> DECIMAL(p,s))
#  - PRIMARY KEY constraints are not supported in Delta Lake; they are
#    mentioned as a comment for reference.
#  - The table is created as a Delta table so that it can be written to
#    and queried efficiently (default location will be used).
# --------------------------------------------------------------

# Drop the table if it already exists to avoid "table already exists" errors.
spark.sql(f"""
  DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBICustInvoiceJourStaging`;
""")

# COMMAND ----------

# Create the staging table with the same column names and types as the source
spark.sql(f"""
  CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICustInvoiceJourStaging` (
      `DEFINITIONGROUP` STRING NOT NULL,
      `EXECUTIONID`    STRING NOT NULL,
      `ISSELECTED`     INT    NOT NULL,
      `TRANSFERSTATUS` INT    NOT NULL,
      `INVOICEID`      STRING NOT NULL,
      `INVOICEDATE`    TIMESTAMP NOT NULL,
      `COMPANY`        STRING NOT NULL,
      `DLVMODE`        STRING NOT NULL,
      `DLVTERM`        STRING NOT NULL,
      `INVOICEACCOUNT` STRING NOT NULL,
      `ORDERACCOUNT`   STRING NOT NULL,
      `PAYMENT`        STRING NOT NULL,
      `CUSTINVOICEJOURRECID` BIGINT NOT NULL,
      `SALESID`        STRING NOT NULL,
      `INVOICEAMOUNT`  DECIMAL(32,6) NOT NULL,
      `CUSTINVOICEJOURCREATEDDATETIME` TIMESTAMP NOT NULL,
      `SALESBALANCE`   DECIMAL(32,6) NOT NULL,
      `SUMMARKUP`      DECIMAL(32,6) NOT NULL,
      `SUMTAX`         DECIMAL(32,6) NOT NULL,
      `PARTITION`      STRING NOT NULL,
      `YSLELEDGERVOUCHER` STRING NOT NULL,
      `DATAAREAID`     STRING NOT NULL,
      `SYNCSTARTDATETIME` TIMESTAMP NOT NULL,
      `RECID`          BIGINT NOT NULL
  )
  USING delta
  -- Location is optional; if omitted Spark will store the Delta table in
  -- the default location for the database under the specified catalog/schema.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
