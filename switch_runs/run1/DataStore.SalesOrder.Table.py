# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.SalesOrder.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Set the catalog to a known accessible catalog (adjust as needed)
# ------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# ------------------------------------------------------------
# Create the target database if it does not exist
# ------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS spark_catalog.DataStore
""")

# COMMAND ----------

# ------------------------------------------------------------
# Create the SalesOrder table using Delta format.
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS spark_catalog.DataStore.SalesOrder (
    SalesOrderCode STRING NOT NULL,
    SalesOrderLineNumber DECIMAL(32,16) NOT NULL,
    SalesOrderLineNumberCombination STRING,
    OrderTransaction STRING,
    DeliveryAddress STRING NOT NULL,
    DocumentStatus STRING NOT NULL,
    HeaderRecId BIGINT NOT NULL,
    LineRecId BIGINT NOT NULL,
    DefaultDimension BIGINT NOT NULL,
    CompanyCode STRING,
    InventTransCode STRING NOT NULL,
    InventDimCode STRING NOT NULL,
    OrderCustomerCode STRING NOT NULL,
    CustomerCode STRING NOT NULL,
    ProductCode STRING NOT NULL,
    DeliveryModeCode STRING NOT NULL,
    PaymentTermsCode STRING NOT NULL,
    DeliveryTermsCode STRING NOT NULL,
    SalesOrderStatus STRING NOT NULL,
    TransactionCurrencyCode STRING,
    CreationDate DATE,
    RequestedShippingDate DATE,
    ConfirmedShippingDate DATE,
    RequestedDeliveryDate DATE,
    ConfirmedDeliveryDate DATE,
    FirstShipmentDate DATE,
    LastShipmentDate DATE,
    SalesUnit STRING NOT NULL,
    OrderedQuantity DECIMAL(32,6) NOT NULL,
    OrderedQuantityRemaining DECIMAL(32,6) NOT NULL,
    DeliveredQuantity DECIMAL(33,6),
    SalesPricePerUnitTC DECIMAL(38,6),
    GrossSalesTC DECIMAL(38,6),
    DiscountAmountTC DECIMAL(38,6),
    InvoicedSalesAmountTC DECIMAL(38,6),
    MarkupAmountTC DECIMAL(38,6),
    NetSalesTC DECIMAL(38,6)
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
