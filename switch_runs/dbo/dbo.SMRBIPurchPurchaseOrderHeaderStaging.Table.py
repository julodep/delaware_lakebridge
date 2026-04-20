# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPurchPurchaseOrderHeaderStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPurchPurchaseOrderHeaderStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table for purchase orders in the target catalog
# ------------------------------------------------------------------
# NOTE: Spark/Databricks does not currently enforce primary‑key
# constraints like SQL Server does.  We create the table with the
# desired schema and add a comment indicating where PK logic would
# normally reside.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIPurchPurchaseOrderHeaderStaging` (
    DEFINITIONGROUP              STRING   NOT NULL,
    EXECUTIONID                  STRING   NOT NULL,
    ISSELECTED                   INT      NOT NULL,
    TRANSFERSTATUS                INT      NOT NULL,
    PURCHASEORDERSTATUS           INT      NOT NULL,
    ORDERVENDORACCOUNTNUMBER     STRING   NOT NULL,
    REQUESTEDDELIVERYDATE        TIMESTAMP NOT NULL,
    TOTALDISCOUNTPERCENTAGE      DECIMAL(32,6) NOT NULL,
    DELIVERYMODEID                STRING   NOT NULL,
    DELIVERYTERMSID               STRING   NOT NULL,
    INVOICEVENDORACCOUNTNUMBER    STRING   NOT NULL,
    PAYMENTTERMSNAME              STRING   NOT NULL,
    PURCHASEORDERNUMBER           STRING   NOT NULL,
    VENDORORDERREFERENCE          STRING   NOT NULL,
    DELIVERYADDRESSCITY          STRING   NOT NULL,
    DELIVERYADDRESSCOUNTRYREGIONID STRING   NOT NULL,
    DELIVERYADDRESSSTREET        STRING   NOT NULL,
    DELIVERYADDRESSSTREETNUMBER  STRING   NOT NULL,
    CURRENCYCODE                 STRING   NOT NULL,
    DELIVERYADDRESSZIPCODE       STRING   NOT NULL,
    PURCHTABLECREATEDDATETIME    TIMESTAMP NOT NULL,
    COMPANY                      STRING   NOT NULL,
    PURCHTABLERECID               BIGINT   NOT NULL,
    DOCUMENTSTATUS                INT      NOT NULL,
    PURCHASETYPE                  INT      NOT NULL,
    WORKERPURCHPLACER             BIGINT   NOT NULL,
    ITEMBUYERGROUPID              STRING   NOT NULL,
    PARTITION                     STRING   NOT NULL,
    DATAAREAID                    STRING   NOT NULL,
    SYNCSTARTDATETIME             TIMESTAMP NOT NULL
)
-- Primary key in SQL Server: (EXECUTIONID, PURCHASEORDERNUMBER, DATAAREAID, PARTITION)
-- Delta does not enforce PKs natively; see comment above.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
