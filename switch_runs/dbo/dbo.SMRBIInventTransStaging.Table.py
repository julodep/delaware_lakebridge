# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventTransStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
# Create the staging table SMRBIInventTransStaging in Delta Lake.
#
# 1. All column data types are mapped to Spark SQL equivalents.
# 2. The composite primary key defined in T‑SQL is not supported by
#    Delta Lake, so it is omitted.  If you need uniqueness guarantees,
#    consider adding a unique index in downstream logic or enforce it
#    programmatically.
# 3. The table is created with `USING DELTA` so that it can be stored
#    in Unity Catalog and benefit from ACID transactions and time‑travel.
# --------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.SMRBIInventTransStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    COSTAMOUNTADJUSTMENT DECIMAL(32, 6) NOT NULL,
    COSTAMOUNTOPERATIONS DECIMAL(32, 6) NOT NULL,
    COSTAMOUNTPHYSICAL DECIMAL(32, 6) NOT NULL,
    COSTAMOUNTPOSTED DECIMAL(32, 6) NOT NULL,
    COSTAMOUNTSETTLED   DECIMAL(32, 16) NOT NULL,
    COSTAMOUNTSTD      DECIMAL(32, 6) NOT NULL,
    CURRENCYCODE STRING NOT NULL,
    COMPANY STRING NOT NULL,
    DATECLOSED TIMESTAMP NOT NULL,
    DATEEXPECTED TIMESTAMP NOT NULL,
    DATEFINANCIAL TIMESTAMP NOT NULL,
    DATEINVENT TIMESTAMP NOT NULL,
    DATEPHYSICAL TIMESTAMP NOT NULL,
    INVENTDIMID STRING NOT NULL,
    INVENTTRANSORIGIN BIGINT NOT NULL,
    INVOICEID STRING NOT NULL,
    ITEMID STRING NOT NULL,
    QTY DECIMAL(32, 6) NOT NULL,
    QTYSETTLED DECIMAL(32, 16) NOT NULL,
    INVENTTRANSRECID BIGINT NOT NULL,
    SHIPPINGDATECONFIRMED TIMESTAMP NOT NULL,
    SHIPPINGDATEREQUESTED TIMESTAMP NOT NULL,
    STATUSISSUE INT NOT NULL,
    STATUSRECEIPT INT NOT NULL,
    VOUCHER STRING NOT NULL,
    VOUCHERPHYSICAL STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
