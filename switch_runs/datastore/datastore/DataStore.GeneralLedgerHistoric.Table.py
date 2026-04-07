# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.GeneralLedgerHistoric.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.GeneralLedgerHistoric.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------
# 1️⃣  Create the GeneralLedgerHistoric table in Unity
# ---------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`GeneralLedgerHistoric` (
    RecId                      INT,
    TransactionCode            STRING,
    CompanyCode                 STRING,
    DefaultExchangeRateTypeCode STRING,
    BudgetExchangeRateTypeCode  STRING,
    TransactionCurrencyCode    STRING,
    AccountingCurrencyCode     STRING,
    ReportingCurrencyCode      STRING,      -- NOT NULL in T‑SQL
    GroupCurrencyCode          STRING,      -- NOT NULL
    GLAccountCode              STRING,
    InterCompanyCode           STRING,
    BusinessSegmentCode        STRING,
    DepartmentCode             STRING,
    EndCustomerCode            STRING,
    LocationCode               STRING,
    ShipmentContractCode       STRING,
    LocalAccountCode           STRING,
    ProductFDCode              STRING,
    DocumentDate                TIMESTAMP,   -- DATETIME2(7) → TIMESTAMP
    DimPostingDateId           INT,
    Voucher                    STRING,      -- NOT NULL
    AmountTC                   DECIMAL(38,6),   -- NOT NULL
    AmountAC                   DECIMAL(38,6),   -- NULL in source
    AmountRC                   DECIMAL(38,6),   -- NULL in source
    AmountGC                   DECIMAL(38,6),   -- NOT NULL
    AppliedExchangeRateTC      DECIMAL(38,21),  -- NOT NULL
    AppliedExchangeRateAC      DECIMAL(38,17),  -- NULL in source
    AppliedExchangeRateRC      DECIMAL(38,17),  -- NULL in source
    AppliedExchangeRateGC      DECIMAL(38,21)   -- NOT NULL
)
COMMENT = 'Historical General Ledger data (converted from T‑SQL)';
""")

# COMMAND ----------

# ---------------------------------------------------------
# 2️⃣  Verify the table was created
# ---------------------------------------------------------

spark.sql(f"SHOW TABLES LIKE 'GeneralLedgerHistoric' IN `dbe_dbx_internships`.`datastore`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1572)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`GeneralLedgerHistoric` (     RecId                      INT,     TransactionCode            STRING,     CompanyCode                 STRING,     DefaultExchangeRateTypeCode STRING,     BudgetExchangeRateTypeCode  STRING,     TransactionCurrencyCode    STRING,     AccountingCurrencyCode     STRING,     ReportingCurrencyCode      STRING,      -- NOT NULL in T‑SQL     GroupCurrencyCode          STRING,      -- NOT NULL     GLAccountCode              STRING,     InterCompanyCode           STRING,     BusinessSegmentCode        STRING,     DepartmentCode             STRING,     EndCustomerCode            STRING,     LocationCode               STRING,     ShipmentContractCode       STRING,     LocalAccountCode           STRING,     ProductFDCode              STRING,     DocumentDate                TIMESTAMP,   -- DATETIME2(7) → TIMESTAMP     DimPostingDateId           INT,     Voucher                    STRING,      -- NOT NULL     AmountTC                   DECIMAL(38,6),   -- NOT NULL     AmountAC                   DECIMAL(38,6),   -- NULL in source     AmountRC                   DECIMAL(38,6),   -- NULL in source     AmountGC                   DECIMAL(38,6),   -- NOT NULL     AppliedExchangeRateTC      DECIMAL(38,21),  -- NOT NULL     AppliedExchangeRateAC      DECIMAL(38,17),  -- NULL in source     AppliedExchangeRateRC      DECIMAL(38,17),  -- NULL in source     AppliedExchangeRateGC      DECIMAL(38,21)   -- NOT NULL ) COMMENT = 'Historical General Ledger data (converted from T‑SQL)';
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'IN'. SQLSTATE: 42601 (line 1, pos 49)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN SHOW TABLES LIKE 'GeneralLedgerHistoric' IN `_placeholder_`.`_placeholder_`
# MAGIC -------------------------------------------------^^^
# MAGIC
# MAGIC ```
