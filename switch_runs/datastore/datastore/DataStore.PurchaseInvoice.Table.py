# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PurchaseInvoice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.PurchaseInvoice.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the `PurchaseInvoice` table in the target catalog and schema.
# All column definitions are mapped from T‑SQL to Spark SQL types:
#   * nvarchar → STRING
#   * bigint   → BIGINT
#   * int      → INT
#   * numeric  → DECIMAL(p, s)
#   * datetime → TIMESTAMP
# Non‑nullable columns are marked with NOT NULL; nullable ones are left
# as the default (nullable = true).
# Note: PRIMARY KEY / FOREIGN KEY constraints from the original
# T‑SQL script are omitted because Delta Lake does not enforce them.
# ------------------------------------------------------------------

spark.sql("""
CREATE TABLE `dbe_dbx_internships`.`datastore`.PurchaseInvoice (
  PurchaseInvoiceCode          STRING,
  InternalInvoiceCode         STRING NOT NULL,
  TransactionType              STRING,
  PurchaseInvoiceLineNumber    DECIMAL(32, 16) NOT NULL,
  InvoiceLineNumberCombination STRING,
  LineDescription              STRING NOT NULL,
  LineRecId                    BIGINT NOT NULL,
  HeaderRecId                  BIGINT NOT NULL,
  CompanyCode                  STRING,
  ProductCode                   STRING NOT NULL,
  PurchaseOrderCode             STRING NOT NULL,
  InventTransCode               STRING NOT NULL,
  InventDimCode                 STRING NOT NULL,
  TaxWriteCode                  INT,
  SupplierCode                   STRING,
  DeliveryModeCode              STRING,
  PaymentTermsCode              STRING,
  DeliveryTermsCode            STRING,
  PurchaseOrderStatus           STRING,
  TransactionCurrencyCode      STRING,
  DefaultDimension              BIGINT NOT NULL,
  InvoiceDate                   TIMESTAMP NOT NULL,
  PurchaseUnit                  STRING,
  InvoicedQuantity              DECIMAL(38, 12),
  PurchasePricePerUnitTC        DECIMAL(38, 6),
  GrossPurchaseTC                DECIMAL(38, 6),
  DiscountAmountTC              DECIMAL(38, 6),
  InvoicedPurchaseAmountTC      DECIMAL(38, 6),
  MarkupAmountTC                DECIMAL(38, 6),
  NetPurchaseTC                 DECIMAL(38, 6),
  NetPurchaseInclTaxTC           DECIMAL(38, 6)
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
