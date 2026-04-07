# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AnalyticalDimensionLedgerSalesAndPurchase.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.AnalyticalDimensionLedgerSalesAndPurchase.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------------
#  Create or replace the AnalyticalDimensionLedgerSalesAndPurchase table
#  in the target Unity Catalog catalog/scheme.
# --------------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`datastore`.`AnalyticalDimensionLedgerSalesAndPurchase`
(
    DefaultDimensionId BIGINT   NOT NULL,
    MainAccount        STRING   NOT NULL,
    Intercompany       STRING   NOT NULL,
    BusinessSegment    STRING   NOT NULL,
    EndCustomer        STRING   NOT NULL,
    Department         STRING   NOT NULL,
    LocalAccount       STRING   NOT NULL,
    Location           STRING   NOT NULL,
    Product            STRING   NOT NULL,
    ShipmentContract   STRING   NOT NULL,
    Vendor             STRING   NOT NULL
)
USING DELTA;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
