# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AccountsPayable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.AccountsPayable.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Create the AccountsPayable staging table in the target Unity
# Catalog and Schema (`dbe_dbx_internships` and `datastore` are placeholders that
# will be replaced by the actual names before execution).
#
# The T‑SQL `nvarchar`/`varchar` columns are mapped to Spark `STRING`.
# The `datetime` columns become `TIMESTAMP`.  
# `BIGINT`  → `LONG`, `INT` → `INT`, and all `decimal`/`numeric`
# types are mapped to `DECIMAL(p,s)` using the precision/scale from
# the original definition.
# ------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`AccountsPayable` (
    AccountsPayableCodeScreening STRING,
    RecId LONG,
    PurchaseInvoiceCode STRING,
    PayablesVoucher STRING,
    Description STRING,
    CompanyCode STRING,
    DimIsOpenAmountId INT,
    OutStandingPeriodCode STRING,
    SupplierCode STRING,

    TransactionCurrencyCode STRING,
    AccountingCurrencyCode STRING,
    ReportingCurrencyCode STRING,
    GroupCurrencyCode STRING,

    InvoiceDate TIMESTAMP,
    DueDate TIMESTAMP,
    LastPaymentDate TIMESTAMP,
    DocumentDate TIMESTAMP,
    ReportDate INT,

    InvoiceAmountTC DECIMAL(22,6),
    InvoiceAmountAC DECIMAL(22,6),
    InvoiceAmountRC DECIMAL(38,6),
    InvoiceAmountGC DECIMAL(38,6),
    InvoiceAmountAC_Budget DECIMAL(38,6),
    InvoiceAmountRC_Budget DECIMAL(38,6),
    InvoiceAmountGC_Budget DECIMAL(38,6),

    PaidAmountTC DECIMAL(38,6),
    PaidAmountAC DECIMAL(38,6),
    PaidAmountRC DECIMAL(38,6),
    PaidAmountGC DECIMAL(38,6),
    PaidAmountAC_Budget DECIMAL(38,6),
    PaidAmountRC_Budget DECIMAL(38,6),
    PaidAmountGC_Budget DECIMAL(38,6),

    OpenAmountTC DECIMAL(38,6),
    OpenAmountAC DECIMAL(38,6),
    OpenAmountRC DECIMAL(38,6),
    OpenAmountGC DECIMAL(38,6),
    OpenAmountAC_Budget DECIMAL(38,6),
    OpenAmountRC_Budget DECIMAL(38,6),
    OpenAmountGC_Budget DECIMAL(38,6),

    AppliedExchangeRateTC DECIMAL(38,6),
    AppliedExchangeRateRC DECIMAL(38,17),
    AppliedExchangeRateAC DECIMAL(38,17),
    AppliedExchangeRateGC DECIMAL(38,17),
    AppliedExchangeRateRC_Budget DECIMAL(38,17),
    AppliedExchangeRateAC_Budget DECIMAL(38,17),
    AppliedExchangeRateGC_Budget DECIMAL(38,17)
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
