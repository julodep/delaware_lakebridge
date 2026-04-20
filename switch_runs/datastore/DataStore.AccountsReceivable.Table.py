# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AccountsReceivable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastoredemo/DataStore.AccountsReceivable.Table.sql`

# COMMAND ----------

# ---------------------------------------------
# 1️⃣  Drop the table if it already exists
# ---------------------------------------------
# In Databricks and Athena the `DROP TABLE IF EXISTS` command is supported.
# Fully‑qualify the table name with the catalog and schema placeholders.
spark.sql(
    f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`datastore`.`AccountsReceivable`"
)

# COMMAND ----------

# ----------------------------------------------------------
# 2️⃣  Re‑create the table with the correct Spark SQL data types
# ----------------------------------------------------------
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.`datastore`.`AccountsReceivable` (
        -- Primary key (non‑clustered in SQL Server, just a hint in Spark)
        AccountsReceivableIdScreening STRING,
        RecId LONG,
        ReceivablesVoucher STRING,
        Description STRING,
        SalesInvoiceCode STRING,
        CompanyCode STRING,
        DimIsOpenAmountId INT,
        OutStandingPeriodCode STRING,
        Test TIMESTAMP,                           -- Nullable
        CustomerCode STRING,                       -- Nullable
        DefaultExchangeRateTypeCode STRING,
        BudgetExchangeRateTypeCode STRING,
        TransactionCurrencyCode STRING,
        AccountingCurrencyCode STRING,
        ReportingCurrencyCode STRING,
        GroupCurrencyCode STRING,
        InvoiceDate TIMESTAMP,
        DueDate TIMESTAMP,                        -- Nullable
        LastPaymentDate TIMESTAMP,
        DocumentDate TIMESTAMP,                    -- Nullable
        ReportDate INT,                            -- Nullable
        InvoiceAmountTC DECIMAL(22,6),              -- Nullable
        InvoiceAmountAC DECIMAL(38,6),
        InvoiceAmountRC DECIMAL(38,6),
        InvoiceAmountGC DECIMAL(38,6),
        InvoiceAmountAC_Budget DECIMAL(38,6),
        InvoiceAmountRC_Budget DECIMAL(38,6),
        InvoiceAmountGC_Budget DECIMAL(38,6),
        PaidAmountTC DECIMAL(38,6),                 -- Nullable
        PaidAmountAC DECIMAL(38,6),
        PaidAmountRC DECIMAL(38,6),
        PaidAmountGC DECIMAL(38,6),
        PaidAmountAC_Budget DECIMAL(38,6),
        PaidAmountRC_Budget DECIMAL(38,6),
        PaidAmountGC_Budget DECIMAL(38,6),
        OpenAmountTC DECIMAL(38,6),                 -- Nullable
        OpenAmountAC DECIMAL(38,6),
        OpenAmountRC DECIMAL(38,6),
        OpenAmountGC DECIMAL(38,6),
        OpenAmountAC_Budget DECIMAL(38,6),
        OpenAmountRC_Budget DECIMAL(38,6),
        OpenAmountGC_Budget DECIMAL(38,6),
        AppliedExchangeRateTC DECIMAL(38,6),         -- Nullable
        AppliedExchangeRateAC DECIMAL(38,17),
        AppliedExchangeRateRC DECIMAL(38,17),
        AppliedExchangeRateGC DECIMAL(38,17),
        AppliedExchangeRateAC_Budget DECIMAL(38,17),
        AppliedExchangeRateRC_Budget DECIMAL(38,17),
        AppliedExchangeRateGC_Budget DECIMAL(38,17)
    )
    """
)

# COMMAND ----------

# ------------------------------
# 3️⃣  Verify the creation
# ------------------------------
# Show the schema of the newly created table to confirm data types
spark.sql(
    f"DESC DataStore.`AccountsReceivable`"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2325)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`AccountsReceivable` (         -- Primary key (non‑clustered in SQL Server, just a hint in Spark)         AccountsReceivableIdScreening STRING,         RecId LONG,         ReceivablesVoucher STRING,         Description STRING,         SalesInvoiceCode STRING,         CompanyCode STRING,         DimIsOpenAmountId INT,         OutStandingPeriodCode STRING,         Test TIMESTAMP,                           -- Nullable         CustomerCode STRING,                       -- Nullable         DefaultExchangeRateTypeCode STRING,         BudgetExchangeRateTypeCode STRING,         TransactionCurrencyCode STRING,         AccountingCurrencyCode STRING,         ReportingCurrencyCode STRING,         GroupCurrencyCode STRING,         InvoiceDate TIMESTAMP,         DueDate TIMESTAMP,                        -- Nullable         LastPaymentDate TIMESTAMP,         DocumentDate TIMESTAMP,                    -- Nullable         ReportDate INT,                            -- Nullable         InvoiceAmountTC DECIMAL(22,6),              -- Nullable         InvoiceAmountAC DECIMAL(38,6),         InvoiceAmountRC DECIMAL(38,6),         InvoiceAmountGC DECIMAL(38,6),         InvoiceAmountAC_Budget DECIMAL(38,6),         InvoiceAmountRC_Budget DECIMAL(38,6),         InvoiceAmountGC_Budget DECIMAL(38,6),         PaidAmountTC DECIMAL(38,6),                 -- Nullable         PaidAmountAC DECIMAL(38,6),         PaidAmountRC DECIMAL(38,6),         PaidAmountGC DECIMAL(38,6),         PaidAmountAC_Budget DECIMAL(38,6),         PaidAmountRC_Budget DECIMAL(38,6),         PaidAmountGC_Budget DECIMAL(38,6),         OpenAmountTC DECIMAL(38,6),                 -- Nullable         OpenAmountAC DECIMAL(38,6),         OpenAmountRC DECIMAL(38,6),         OpenAmountGC DECIMAL(38,6),         OpenAmountAC_Budget DECIMAL(38,6),         OpenAmountRC_Budget DECIMAL(38,6),         OpenAmountGC_Budget DECIMAL(38,6),         AppliedExchangeRateTC DECIMAL(38,6),         -- Nullable         AppliedExchangeRateAC DECIMAL(38,17),         AppliedExchangeRateRC DECIMAL(38,17),         AppliedExchangeRateGC DECIMAL(38,17),         AppliedExchangeRateAC_Budget DECIMAL(38,17),         AppliedExchangeRateRC_Budget DECIMAL(38,17),         AppliedExchangeRateGC_Budget DECIMAL(38,17)     )
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
