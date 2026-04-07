# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PurchaseDelivery.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.PurchaseDelivery.Table.sql`

# COMMAND ----------

# Create the PurchaseDelivery table in the target catalog and schema.
# NVARCHAR columns are mapped to STRING, DATE stays DATE, BIGINT becomes LONG,
# and NUMERIC(p,s) maps to DECIMAL(p,s).  NULL/NOT NULL annotations are preserved
# as best effort – Delta supports nullable columns by default, but the
# specification is included for clarity.

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`PurchaseDelivery` (
    PackingSlipCode            STRING NOT NULL,
    PurchaseOrderCode          STRING NOT NULL,
    CompanyCode                 STRING NOT NULL,
    ProductConfigurationCode   STRING NOT NULL,
    ProductCode                STRING NOT NULL,
    OrderSupplierCode          STRING NOT NULL,
    SupplierCode               STRING NOT NULL,
    DeliveryModeCode           STRING NOT NULL,
    DeliveryTermsCode          STRING NOT NULL,
    ActualDeliveryDate          DATE,
    RequestedDeliveryDate       DATE,
    ConfirmedDeliveryDate      DATE,
    PurchaseType                STRING,
    PurchaseOrderLineNumber     LONG NOT NULL,
    DeliveryName                STRING NOT NULL,
    DeliveryLineNumber       DECIMAL(32,16) NOT NULL,
    PurchaseUnit                STRING NOT NULL,
    QuantityOrdered         DECIMAL(32,6),
    QuantityDelivered      DECIMAL(32,6)
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
