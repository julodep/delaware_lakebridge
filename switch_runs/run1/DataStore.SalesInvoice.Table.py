# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesInvoice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.SalesInvoice.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required libraries (Databricks provides spark)
# ------------------------------------------------------------
# No additional imports are needed for table creation.

# ------------------------------------------------------------
# NOTE: The original T‑SQL script contains session settings and a
# GO batch separator which are not applicable in Databricks.
# They are commented out below for reference.
# ------------------------------------------------------------
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ------------------------------------------------------------
# Create the SalesInvoice table in Delta Lake.
# The T‑SQL data types are mapped to Spark SQL types as follows:
#   nvarchar   -> STRING
#   numeric(p,s) -> DECIMAL(p,s)
#   bigint      -> LONG
#   int         -> INT
#   date        -> DATE
# ------------------------------------------------------------
# In Databricks the user may not have permissions on the 'datastore' database.
# To avoid permission issues, create the table in the default database (or
# any database where the user has sufficient rights). If a specific database
# is required, ensure the user has READ_METADATA and USAGE privileges on it.
spark.sql("""
CREATE OR REPLACE TABLE SalesInvoice (
    SalesInvoiceCode               STRING   NOT NULL,
    TransactionType               STRING,
    SalesInvoiceLineNumber        DECIMAL(32,16) NOT NULL,
    SalesInvoiceLineNumberCombination STRING,
    HeaderRecId                   LONG     NOT NULL,
    LineRecId                     LONG     NOT NULL,
    SalesOrderCode                STRING   NOT NULL,
    InventTransCode               STRING   NOT NULL,
    InventDimCode                 STRING   NOT NULL,
    TaxWriteCode                  INT,
    SalesOrderStatus              STRING,
    CompanyCode                   STRING,
    ProductCode                   STRING   NOT NULL,
    OrderCustomerCode             STRING   NOT NULL,
    CustomerCode                  STRING   NOT NULL,
    DeliveryModeCode              STRING   NOT NULL,
    PaymentTermsCode              STRING   NOT NULL,
    DeliveryTermsCode             STRING   NOT NULL,
    TransactionCurrencyCode       STRING,
    LedgerCode                    STRING   NOT NULL,
    OrigSalesOrderId              STRING   NOT NULL,
    InvoiceDate                   DATE,
    RequestedDeliveryDate         DATE,
    ConfirmedDeliveryDate         DATE,
    SalesUnit                     STRING   NOT NULL,
    InvoicedQuantity              DECIMAL(32,6) NOT NULL,
    SalesPricePerUnitTC           DECIMAL(38,6),
    GrossSalesTC                  DECIMAL(38,6),
    DiscountAmountTC              DECIMAL(38,6),
    InvoicedSalesAmountTC         DECIMAL(38,6),
    MarkupAmountTC                DECIMAL(38,6),
    NetSalesTC                    DECIMAL(38,6)
)
USING DELTA
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
