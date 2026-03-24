# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PurchaseOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.PurchaseOrder.Table.sql`

# COMMAND ----------

# Import any required libraries (Databricks provides Spark session by default)
import sys

# COMMAND ----------

# ------------------------------------------------------------
# Create the PurchaseOrder table in the default catalog/database
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE PurchaseOrder (
    PurchaseOrderCode STRING,
    RecId BIGINT NOT NULL,
    PurchaseOrderLineNumber BIGINT NOT NULL,
    OrderLineNumberCombination STRING,
    DeliveryAddress STRING NOT NULL,
    CompanyCode STRING,
    InventTransCode STRING NOT NULL,
    ProductCode STRING NOT NULL,
    SupplierCode STRING,
    OrderSupplierCode STRING,
    DeliveryModeCode STRING NOT NULL,
    PaymentTermsCode STRING NOT NULL,
    DeliveryTermsCode STRING NOT NULL,
    PurchaseOrderStatus STRING NOT NULL,
    TransactionCurrencyCode STRING,
    InventDimCode STRING NOT NULL,
    CreationDate DATE,
    RequestedDeliveryDate TIMESTAMP NOT NULL,
    ConfirmedDeliveryDate TIMESTAMP NOT NULL,
    OrderedQuantity DECIMAL(32,6) NOT NULL,
    OrderedQuantityRemaining DECIMAL(32,6) NOT NULL,
    DeliveredQuantity DECIMAL(33,6),
    PurchaseUnit STRING NOT NULL,
    PurchasePricePerUnitTC DECIMAL(38,6) NOT NULL,
    GrossPurchaseTC DECIMAL(38,6),
    DiscountAmountTC DECIMAL(38,6),
    InvoicedPurchaseAmountTC DECIMAL(38,6),
    MarkupAmountTC DECIMAL(38,6),
    NetPurchaseTC DECIMAL(38,6)
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
