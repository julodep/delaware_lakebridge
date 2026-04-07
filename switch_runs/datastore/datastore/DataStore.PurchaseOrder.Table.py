# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PurchaseOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.PurchaseOrder.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Table creation – PurchaseOrder
# ------------------------------------------------------------------
# The original T‑SQL table was created in the [DataStore] schema.
# In Databricks we create a Delta table called `PurchaseOrder` under
# the target catalog and schema (`dbe_dbx_internships` and `datastore`) using
# Spark SQL.  All T‑SQL data types are mapped to Spark SQL types:
#   • BIGINT        -> LONG
#   • NVARCHAR      -> STRING
#   • DATETIME      -> TIMESTAMP
#   • NUMERIC(p,s)  -> DECIMAL(p,s)
#   • DATE          -> DATE
# The NOT NULL constraints from the original definition are preserved.
# ------------------------------------------------------------------

spark.sql(
"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`PurchaseOrder`
(
    PurchaseOrderCode            STRING,
    RecId                         LONG NOT NULL,
    PurchaseOrderLineNumber       LONG NOT NULL,
    OrderLineNumberCombination    STRING,
    DeliveryAddress              STRING NOT NULL,
    CompanyCode                  STRING,
    InventTransCode              STRING NOT NULL,
    ProductCode                  STRING NOT NULL,
    SupplierCode                  STRING,
    OrderSupplierCode             STRING,
    DeliveryModeCode              STRING NOT NULL,
    PaymentTermsCode              STRING NOT NULL,
    DeliveryTermsCode             STRING NOT NULL,
    PurchaseOrderStatus           STRING NOT NULL,
    TransactionCurrencyCode       STRING,
    InventDimCode                 STRING NOT NULL,
    CreationDate                  DATE,
    RequestedDeliveryDate          TIMESTAMP NOT NULL,
    ConfirmedDeliveryDate          TIMESTAMP NOT NULL,
    OrderedQuantity                 DECIMAL(32,6) NOT NULL,
    OrderedQuantityRemaining      DECIMAL(32,6) NOT NULL,
    DeliveredQuantity             DECIMAL(33,6),
    PurchaseUnit                   STRING NOT NULL,
    PurchasePricePerUnitTC         DECIMAL(38,6) NOT NULL,
    GrossPurchaseTC                DECIMAL(38,6),
    DiscountAmountTC               DECIMAL(38,6),
    InvoicedPurchaseAmountTC        DECIMAL(38,6),
    MarkupAmountTC                 DECIMAL(38,6),
    NetPurchaseTC                  DECIMAL(38,6) NOT NULL
)
"""
)

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: verify the schema by showing the table definition
# ------------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`datastore`.`PurchaseOrder`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
