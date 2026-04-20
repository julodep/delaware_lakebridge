# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIEcoResReleasedProductStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIEcoResReleasedProductStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table SMRBIEcoResReleasedProductStaging
# ------------------------------------------------------------------
# The original T‑SQL statement includes a primary key constraint that
# Delta Lake (used by Databricks) does not enforce. We create the
# table with the same columns and add a comment indicating that the
# primary key is not enforced. If you need uniqueness guarantees,
# enforce them at the application level or by adding a unique index
# via a Delta Lake OPTIMIZE and ZORDER, but that is beyond the scope
# of this translation.
# ------------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIEcoResReleasedProductStaging` (
    DEFINITIONGROUP         STRING  NOT NULL,
    EXECUTIONID             STRING  NOT NULL,
    ISSELECTED              INT     NOT NULL,
    TRANSFERSTATUS          INT     NOT NULL,
    BOMUNITSYMBOL           STRING  NOT NULL,
    ITEMNUMBER              STRING  NOT NULL,
    PRODUCTGROUPID          STRING  NOT NULL,
    PRODUCTNUMBER           STRING  NOT NULL,
    BUYERGROUPID            STRING  NOT NULL,
    INVENTORYUNITSYMBOL     STRING  NOT NULL,
    ORIGINCOUNTRYREGIONID   STRING  NOT NULL,
    PRIMARYVENDORACCOUNTNUMBER STRING NOT NULL,
    REVENUEABCCODE          INT     NOT NULL,
    SALESUNITSYMBOL         STRING  NOT NULL,
    PRODUCTCREATEDDATETIME  TIMESTAMP NOT NULL,
    COMPANY                 STRING  NOT NULL,
    PURCHASEUNITSYMBOL      STRING  NOT NULL,
    PRODUCTRECID            BIGINT  NOT NULL,
    PHYSICALDIMENSIONGROUPID STRING NOT NULL,
    INTRASTATCOMMODITYCODE  STRING  NOT NULL,
    PARTITION               STRING  NOT NULL,
    DATAAREAID              STRING  NOT NULL,
    SYNCSTARTDATETIME       TIMESTAMP NOT NULL
    /* PRIMARY KEY (
       EXECUTIONID,
       ITEMNUMBER,
       DATAAREAID,
       PARTITION
     ) is not supported in Delta Lake; comment retained for reference. */
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
