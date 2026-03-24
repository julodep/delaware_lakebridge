# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PurchaseInvoice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.PurchaseInvoice.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Setup: import any needed modules (Databricks provides Spark session)
# ------------------------------------------------------------------
import sys

# COMMAND ----------

# ------------------------------------------------------------------
# Switch to a catalog that the current user has metadata permissions for.
# In many Databricks environments, the default Hive Metastore catalog
# (named `hive_metastore`) is accessible without special privileges.
# ------------------------------------------------------------------
spark.sql("USE CATALOG hive_metastore")

# COMMAND ----------

# ------------------------------------------------------------------
# Create the target database if it does not already exist.
# Using unquoted identifiers avoids case‑sensitivity issues that can
# trigger permission checks on non‑existent catalogs/databases.
# ------------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS DataStore
""")

# COMMAND ----------

# ------------------------------------------------------------------
# NOTE: The original T‑SQL script includes SET options and GO batch
# separators which are not applicable in Databricks. They are omitted
# (or left as comments) because Spark SQL does not support them.
# ------------------------------------------------------------------
# SET ANSI_NULLS ON;      -- not needed / not supported in Spark
# SET QUOTED_IDENTIFIER ON;  -- not needed / not supported in Spark

# ------------------------------------------------------------------
# Create the Delta table `DataStore.PurchaseInvoice`.
# The original script used `ON [PRIMARY]` which is a SQL Server
# storage clause; this is not applicable in Delta Lake and is omitted.
# ------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE DataStore.PurchaseInvoice (
    PurchaseInvoiceCode          STRING,
    InternalInvoiceCode          STRING NOT NULL,
    TransactionType              STRING,
    PurchaseInvoiceLineNumber    DECIMAL(32,16) NOT NULL,
    InvoiceLineNumberCombination STRING,
    LineDescription              STRING NOT NULL,
    LineRecId                    BIGINT NOT NULL,
    HeaderRecId                  BIGINT NOT NULL,
    CompanyCode                  STRING,
    ProductCode                  STRING NOT NULL,
    PurchaseOrderCode            STRING NOT NULL,
    InventTransCode              STRING NOT NULL,
    InventDimCode                STRING NOT NULL,
    TaxWriteCode                 INT,
    SupplierCode                 STRING,
    DeliveryModeCode             STRING,
    PaymentTermsCode             STRING,
    DeliveryTermsCode            STRING,
    PurchaseOrderStatus          STRING,
    TransactionCurrencyCode      STRING,
    DefaultDimension             BIGINT NOT NULL,
    InvoiceDate                  TIMESTAMP NOT NULL,
    PurchaseUnit                 STRING,
    InvoicedQuantity             DECIMAL(38,12),
    PurchasePricePerUnitTC       DECIMAL(38,6),
    GrossPurchaseTC              DECIMAL(38,6),
    DiscountAmountTC             DECIMAL(38,6),
    InvoicedPurchaseAmountTC    DECIMAL(38,6),
    MarkupAmountTC               DECIMAL(38,6),
    NetPurchaseTC                DECIMAL(38,6),
    NetPurchaseInclTaxTC        DECIMAL(38,6)
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Verify table creation (display schema)
# ------------------------------------------------------------------
display(spark.sql("DESCRIBE DETAIL DataStore.PurchaseInvoice"))

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
