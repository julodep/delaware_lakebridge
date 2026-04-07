# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AccountsReceivable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.AccountsReceivable.Table.sql`

# COMMAND ----------

# Create the AccountsReceivable table in the target Unity Catalog
# All references are fully‑qualified: `dbe_dbx_internships`.`datastore`.`AccountsReceivable`

spark.sql(
    f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`AccountsReceivable` (
    -- 28‑char unique identifier
    AccountsReceivableIdScreening STRING,
    -- Primary key surrogate
    RecId BIGINT,
    -- Voucher reference
    ReceivablesVoucher STRING,
    -- Description of the entry
    Description STRING,
    -- Sales invoice identifiers
    SalesInvoiceCode STRING,
    CompanyCode STRING,
    DimIsOpenAmountId INT,
    OutStandingPeriodCode STRING,
    -- Optional datetime column
    Test TIMESTAMP,
    -- Optional customer code
    CustomerCode STRING,
    -- Exchange‑rate type codes
    DefaultExchangeRateTypeCode STRING,
    BudgetExchangeRateTypeCode STRING,
    -- Currency codes
    TransactionCurrencyCode STRING,
    AccountingCurrencyCode STRING,
    ReportingCurrencyCode STRING,
    GroupCurrencyCode STRING,
    -- Invoice dates
    InvoiceDate TIMESTAMP,
    DueDate TIMESTAMP,
    LastPaymentDate TIMESTAMP,
    DocumentDate TIMESTAMP,
    -- Optional integer field
    ReportDate INT,
    -- Monetary amounts
    InvoiceAmountTC DECIMAL(22,6),
    InvoiceAmountAC DECIMAL(38,6),
    InvoiceAmountRC DECIMAL(38,6),
    InvoiceAmountGC DECIMAL(38,6),
    InvoiceAmountAC_Budget DECIMAL(38,6),
    InvoiceAmountRC_Budget DECIMAL(38,6),
    InvoiceAmountGC_Budget DECIMAL(38,6),
    -- Paid amounts
    PaidAmountTC DECIMAL(38,6),
    PaidAmountAC DECIMAL(38,6),
    PaidAmountRC DECIMAL(38,6),
    PaidAmountGC DECIMAL(38,6),
    PaidAmountAC_Budget DECIMAL(38,6),
    PaidAmountRC_Budget DECIMAL(38,6),
    PaidAmountGC_Budget DECIMAL(38,6),
    -- Outstanding amounts
    OpenAmountTC DECIMAL(38,6),
    OpenAmountAC DECIMAL(38,6),
    OpenAmountRC DECIMAL(38,6),
    OpenAmountGC DECIMAL(38,6),
    OpenAmountAC_Budget DECIMAL(38,6),
    OpenAmountRC_Budget DECIMAL(38,6),
    OpenAmountGC_Budget DECIMAL(38,6),
    -- Applied exchange rates
    AppliedExchangeRateTC DECIMAL(38,6),
    AppliedExchangeRateAC DECIMAL(38,17),
    AppliedExchangeRateRC DECIMAL(38,17),
    AppliedExchangeRateGC DECIMAL(38,17),
    AppliedExchangeRateAC_Budget DECIMAL(38,17),
    AppliedExchangeRateRC_Budget DECIMAL(38,17),
    AppliedExchangeRateGC_Budget DECIMAL(38,17)
)
USING DELTA
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2206)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`AccountsReceivable` (     -- 28‑char unique identifier     AccountsReceivableIdScreening STRING,     -- Primary key surrogate     RecId BIGINT,     -- Voucher reference     ReceivablesVoucher STRING,     -- Description of the entry     Description STRING,     -- Sales invoice identifiers     SalesInvoiceCode STRING,     CompanyCode STRING,     DimIsOpenAmountId INT,     OutStandingPeriodCode STRING,     -- Optional datetime column     Test TIMESTAMP,     -- Optional customer code     CustomerCode STRING,     -- Exchange‑rate type codes     DefaultExchangeRateTypeCode STRING,     BudgetExchangeRateTypeCode STRING,     -- Currency codes     TransactionCurrencyCode STRING,     AccountingCurrencyCode STRING,     ReportingCurrencyCode STRING,     GroupCurrencyCode STRING,     -- Invoice dates     InvoiceDate TIMESTAMP,     DueDate TIMESTAMP,     LastPaymentDate TIMESTAMP,     DocumentDate TIMESTAMP,     -- Optional integer field     ReportDate INT,     -- Monetary amounts     InvoiceAmountTC DECIMAL(22,6),     InvoiceAmountAC DECIMAL(38,6),     InvoiceAmountRC DECIMAL(38,6),     InvoiceAmountGC DECIMAL(38,6),     InvoiceAmountAC_Budget DECIMAL(38,6),     InvoiceAmountRC_Budget DECIMAL(38,6),     InvoiceAmountGC_Budget DECIMAL(38,6),     -- Paid amounts     PaidAmountTC DECIMAL(38,6),     PaidAmountAC DECIMAL(38,6),     PaidAmountRC DECIMAL(38,6),     PaidAmountGC DECIMAL(38,6),     PaidAmountAC_Budget DECIMAL(38,6),     PaidAmountRC_Budget DECIMAL(38,6),     PaidAmountGC_Budget DECIMAL(38,6),     -- Outstanding amounts     OpenAmountTC DECIMAL(38,6),     OpenAmountAC DECIMAL(38,6),     OpenAmountRC DECIMAL(38,6),     OpenAmountGC DECIMAL(38,6),     OpenAmountAC_Budget DECIMAL(38,6),     OpenAmountRC_Budget DECIMAL(38,6),     OpenAmountGC_Budget DECIMAL(38,6),     -- Applied exchange rates     AppliedExchangeRateTC DECIMAL(38,6),     AppliedExchangeRateAC DECIMAL(38,17),     AppliedExchangeRateRC DECIMAL(38,17),     AppliedExchangeRateGC DECIMAL(38,17),     AppliedExchangeRateAC_Budget DECIMAL(38,17),     AppliedExchangeRateRC_Budget DECIMAL(38,17),     AppliedExchangeRateGC_Budget DECIMAL(38,17) ) USING DELTA
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
