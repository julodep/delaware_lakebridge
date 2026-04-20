# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AccountsPayable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastoredemo/DataStore.AccountsPayable.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Create the Translated AccountsPayable table in Unity Catalog
# -------------------------------------------------------------
# All T‑SQL types are converted to Spark SQL types according to the
# mapping table provided in the instructions.  The statement is fully‑
# qualified with dbe_dbx_internships and datastore placeholders so that the notebook
# can be parameterised before execution.
#
# Note: The `ON [PRIMARY]` clause is a SQL Server‑specific storage hint
# and has no equivalent in Delta Lake / Unity Catalog, so it is omitted.

spark.sql("""
CREATE OR REPLACE TABLE DataStore.`AccountsPayable` (
    AccountsPayableCodeScreening STRING NOT NULL,
    RecId LONG NOT NULL,
    PurchaseInvoiceCode STRING NOT NULL,
    PayablesVoucher STRING NOT NULL,
    Description STRING NOT NULL,
    CompanyCode STRING NOT NULL,
    DimIsOpenAmountId INT NOT NULL,
    OutStandingPeriodCode STRING NOT NULL,
    SupplierCode STRING NOT NULL,
    TransactionCurrencyCode STRING,
    AccountingCurrencyCode STRING NOT NULL,
    ReportingCurrencyCode STRING NOT NULL,
    GroupCurrencyCode STRING NOT NULL,
    InvoiceDate TIMESTAMP NOT NULL,
    DueDate TIMESTAMP,
    LastPaymentDate TIMESTAMP NOT NULL,
    DocumentDate TIMESTAMP,
    ReportDate INT,
    InvoiceAmountTC DECIMAL(22,6),
    InvoiceAmountAC DECIMAL(22,6),
    InvoiceAmountRC DECIMAL(38,6) NOT NULL,
    InvoiceAmountGC DECIMAL(38,6) NOT NULL,
    InvoiceAmountAC_Budget DECIMAL(38,6) NOT NULL,
    InvoiceAmountRC_Budget DECIMAL(38,6) NOT NULL,
    InvoiceAmountGC_Budget DECIMAL(38,6) NOT NULL,
    PaidAmountTC DECIMAL(38,6),
    PaidAmountAC DECIMAL(38,6),
    PaidAmountRC DECIMAL(38,6) NOT NULL,
    PaidAmountGC DECIMAL(38,6) NOT NULL,
    PaidAmountAC_Budget DECIMAL(38,6) NOT NULL,
    PaidAmountRC_Budget DECIMAL(38,6) NOT NULL,
    PaidAmountGC_Budget DECIMAL(38,6) NOT NULL,
    OpenAmountTC DECIMAL(38,6),
    OpenAmountAC DECIMAL(38,6),
    OpenAmountRC DECIMAL(38,6) NOT NULL,
    OpenAmountGC DECIMAL(38,6) NOT NULL,
    OpenAmountAC_Budget DECIMAL(38,6) NOT NULL,
    OpenAmountRC_Budget DECIMAL(38,6) NOT NULL,
    OpenAmountGC_Budget DECIMAL(38,6) NOT NULL,
    AppliedExchangeRateTC DECIMAL(38,6),
    AppliedExchangeRateRC DECIMAL(38,17) NOT NULL,
    AppliedExchangeRateAC DECIMAL(38,17) NOT NULL,
    AppliedExchangeRateGC DECIMAL(38,17) NOT NULL,
    AppliedExchangeRateRC_Budget DECIMAL(38,17) NOT NULL,
    AppliedExchangeRateAC_Budget DECIMAL(38,17) NOT NULL,
    AppliedExchangeRateGC_Budget DECIMAL(38,17) NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
