# Databricks notebook source
# MAGIC %md
# MAGIC # DWH.FactAccountsPayable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323121756-cmb3/DWH.FactAccountsPayable.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any necessary modules (Databricks provides Spark)
# ------------------------------------------------------------
# No additional imports required for basic Spark SQL execution

# ------------------------------------------------------------
# Create the FactAccountsPayable Delta table in the DWH schema
# ------------------------------------------------------------
# Note:
# - Delta Lake does not enforce PRIMARY KEY or FOREIGN KEY constraints.
# - IDENTITY columns are not directly supported; use a separate sequence or generate IDs in Python if needed.
# - Bit type is mapped to BOOLEAN.
# - datetime is mapped to TIMESTAMP.
# - NVARCHAR and NUMERIC are mapped to STRING and DECIMAL respectively.
# - Spark SQL does not have a LONG type; use BIGINT instead.

spark.sql("""
CREATE OR REPLACE TABLE `DWH`.`FactAccountsPayable` (
    FactAccountsPayableId INT,                         -- Intended as IDENTITY(1,1) in T‑SQL
    DimCompanyId INT NOT NULL,
    DimPurchaseInvoiceId INT NOT NULL,
    DimIsOpenAmountId BOOLEAN NOT NULL,
    DimOutstandingPeriodId INT NOT NULL,
    DimSupplierId INT NOT NULL,
    DimTransactionCurrencyId INT NOT NULL,
    DimAccountingCurrencyId INT NOT NULL,
    DimReportingCurrencyId INT NOT NULL,
    DimGroupCurrencyId INT NOT NULL,
    DimReportDateId INT NOT NULL,
    DimDueDateId INT NOT NULL,
    ReportMonthId INT,
    InvoiceDate TIMESTAMP NOT NULL,
    LastPaymentDate TIMESTAMP NOT NULL,
    DocumentDate TIMESTAMP NOT NULL,
    RecId BIGINT NOT NULL,                             -- Changed from LONG to BIGINT for Spark compatibility
    PayablesVoucher STRING NOT NULL,
    Description STRING NOT NULL,
    InvoiceAmountTC DECIMAL(38,6),
    InvoiceAmountAC DECIMAL(38,6),
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
    AppliedExchangeRateAC DECIMAL(38,6),
    AppliedExchangeRateRC DECIMAL(38,6),
    AppliedExchangeRateGC DECIMAL(38,6),
    AppliedExchangeRateAC_Budget DECIMAL(38,6),
    AppliedExchangeRateRC_Budget DECIMAL(38,6),
    AppliedExchangeRateGC_Budget DECIMAL(38,6),
    CreatedETLRunId INT NOT NULL,
    ModifiedETLRunId INT NOT NULL,
    DimVoucherId INT
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2049)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `DWH`.`FactAccountsPayable` (     FactAccountsPayableId INT,                         -- Intended as IDENTITY(1,1) in T‑SQL     DimCompanyId INT NOT NULL,     DimPurchaseInvoiceId INT NOT NULL,     DimIsOpenAmountId BOOLEAN NOT NULL,     DimOutstandingPeriodId INT NOT NULL,     DimSupplierId INT NOT NULL,     DimTransactionCurrencyId INT NOT NULL,     DimAccountingCurrencyId INT NOT NULL,     DimReportingCurrencyId INT NOT NULL,     DimGroupCurrencyId INT NOT NULL,     DimReportDateId INT NOT NULL,     DimDueDateId INT NOT NULL,     ReportMonthId INT,     InvoiceDate TIMESTAMP NOT NULL,     LastPaymentDate TIMESTAMP NOT NULL,     DocumentDate TIMESTAMP NOT NULL,     RecId BIGINT NOT NULL,                             -- Changed from LONG to BIGINT for Spark compatibility     PayablesVoucher STRING NOT NULL,     Description STRING NOT NULL,     InvoiceAmountTC DECIMAL(38,6),     InvoiceAmountAC DECIMAL(38,6),     InvoiceAmountRC DECIMAL(38,6),     InvoiceAmountGC DECIMAL(38,6),     InvoiceAmountAC_Budget DECIMAL(38,6),     InvoiceAmountRC_Budget DECIMAL(38,6),     InvoiceAmountGC_Budget DECIMAL(38,6),     PaidAmountTC DECIMAL(38,6),     PaidAmountAC DECIMAL(38,6),     PaidAmountRC DECIMAL(38,6),     PaidAmountGC DECIMAL(38,6),     PaidAmountAC_Budget DECIMAL(38,6),     PaidAmountRC_Budget DECIMAL(38,6),     PaidAmountGC_Budget DECIMAL(38,6),     OpenAmountTC DECIMAL(38,6),     OpenAmountAC DECIMAL(38,6),     OpenAmountRC DECIMAL(38,6),     OpenAmountGC DECIMAL(38,6),     OpenAmountAC_Budget DECIMAL(38,6),     OpenAmountRC_Budget DECIMAL(38,6),     OpenAmountGC_Budget DECIMAL(38,6),     AppliedExchangeRateTC DECIMAL(38,6),     AppliedExchangeRateAC DECIMAL(38,6),     AppliedExchangeRateRC DECIMAL(38,6),     AppliedExchangeRateGC DECIMAL(38,6),     AppliedExchangeRateAC_Budget DECIMAL(38,6),     AppliedExchangeRateRC_Budget DECIMAL(38,6),     AppliedExchangeRateGC_Budget DECIMAL(38,6),     CreatedETLRunId INT NOT NULL,     ModifiedETLRunId INT NOT NULL,     DimVoucherId INT ) USING DELTA
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC ```
