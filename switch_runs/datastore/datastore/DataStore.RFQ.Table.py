# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.RFQ.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.RFQ.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Create the DataStore.RFQ Delta table in the target catalog.
# -------------------------------------------------------------
# NOTE:  
# • `nvachar` → Spark `STRING` (no length restriction in Spark).  
# • `numeric(p,s)` → Spark `DECIMAL(p,s)` (Spark supports up to precision 38).  
# • `bigint` → Spark `LONG`.  
# • `date` → Spark `DATE`.  
# • NOT NULL constraints are not enforced in Delta Lake, but we keep the
#   comments so the schema intent is clear.  
# • Fully‑qualified references use the placeholders `dbe_dbx_internships` and `datastore` 
#   which should be replaced by the actual catalog and schema names before 
#   running the notebook.  
# -------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`RFQ`
USING DELTA
(
    RFQCaseCode STRING,                     -- NVARCHAR(20) NOT NULL
    RFQCaseName STRING,                     -- NVARCHAR(60) NOT NULL
    RFQCaseLineNumber LONG,                 -- BIGINT NOT NULL
    RFQCode STRING,                         -- NVARCHAR(20) NOT NULL
    RFQName STRING,                          -- NVARCHAR(100) NOT NULL
    RFQLineNumber DECIMAL(32,16),            -- NUMERIC(32,16) NOT NULL
    SupplierCode STRING,                    -- NVARCHAR(20) NOT NULL
    CompanyCode STRING,                     -- NVARCHAR(4) NOT NULL
    ProductConfigurationCode STRING,        -- NVARCHAR(20) NOT NULL
    ProductCode STRING,                     -- NVARCHAR(20) NOT NULL
    DeliveryModeCode STRING,                -- NVARCHAR(10) NOT NULL
    DeliveryTermsCode STRING,               -- NVARCHAR(10) NOT NULL
    DefaultExchangeRateTypeCode STRING,      -- NVARCHAR(20) NOT NULL
    BudgetExchangeRateTypeCode STRING,       -- NVARCHAR(20) NOT NULL
    TransactionCurrencyCode STRING,         -- NVARCHAR(4) NOT NULL
    AccountingCurrencyCode STRING,          -- NVARCHAR(3) NOT NULL
    ReportingCurrencyCode STRING,            -- NVARCHAR(3) NOT NULL
    GroupCurrencyCode STRING,               -- NVARCHAR(50) NOT NULL
    DeliveryDate DATE,                      -- DATE NULL
    ExpiryDate DATE,                        -- DATE NULL
    CreatedDate DATE,                       -- DATE NULL
    RFQCaseStatusLow STRING,                -- NVARCHAR(50) NULL
    RFQCaseStatusHigh STRING,               -- NVARCHAR(50) NULL
    RFQLineStatus STRING,                   -- NVARCHAR(50) NULL
    PurchaseUnit STRING,                    -- NVARCHAR(10) NOT NULL
    PurchQuantity DECIMAL(32,6),             -- NUMERIC(32,6) NOT NULL
    PurchPriceTC DECIMAL(32,6),              -- NUMERIC(32,6) NOT NULL
    PurchPriceAC DECIMAL(38,6),              -- NUMERIC(38,6) NOT NULL
    PurchPriceRC DECIMAL(38,6),              -- NUMERIC(38,6) NOT NULL
    PurchPriceGC DECIMAL(38,6),              -- NUMERIC(38,6) NOT NULL
    PurchPriceAC_Budget DECIMAL(38,6),       -- NUMERIC(38,6) NOT NULL
    PurchPriceRC_Budget DECIMAL(38,6),       -- NUMERIC(38,6) NOT NULL
    PurchPriceGC_Budget DECIMAL(38,6),       -- NUMERIC(38,6) NOT NULL
    RFQLineAmountTC DECIMAL(32,6),           -- NUMERIC(32,6) NOT NULL
    RFQLineAmountAC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    RFQLineAmountRC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    RFQLineAmountGC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    RFQLineAmountAC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    RFQLineAmountRC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    RFQLineAmountGC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    CostAvoidanceAmountTC DECIMAL(33,6),     -- NUMERIC(33,6) NOT NULL
    CostAvoidanceAmountAC DECIMAL(38,6),     -- NUMERIC(38,6) NOT NULL
    CostAvoidanceAmountRC DECIMAL(38,6),     -- NUMERIC(38,6) NOT NULL
    CostAvoidanceAmountGC DECIMAL(38,6),     -- NUMERIC(38,6) NOT NULL
    CostAvoidanceAmountAC_Budget DECIMAL(38,6), -- NUMERIC(38,6) NOT NULL
    CostAvoidanceAmountRC_Budget DECIMAL(38,6), -- NUMERIC(38,6) NOT NULL
    CostAvoidanceAmountGC_Budget DECIMAL(38,6), -- NUMERIC(38,6) NOT NULL
    MaxPurchPriceTC DECIMAL(32,6),           -- NUMERIC(32,6) NOT NULL
    MaxPurchPriceAC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    MaxPurchPriceRC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    MaxPurchPriceGC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    MaxPurchPriceAC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    MaxPurchPriceRC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    MaxPurchPriceGC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    MinPurchPriceTC DECIMAL(32,6),           -- NUMERIC(32,6) NOT NULL
    MinPurchPriceAC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    MinPurchPriceRC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    MinPurchPriceGC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL
    MinPurchPriceAC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    MinPurchPriceRC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    MinPurchPriceGC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL
    AppliedExchangeRateTC DECIMAL(38,6),      -- NUMERIC(38,6) NULL
    AppliedExchangeRateRC DECIMAL(38,21),     -- NUMERIC(38,21) NOT NULL
    AppliedExchangeRateAC DECIMAL(38,21),     -- NUMERIC(38,21) NOT NULL
    AppliedExchangeRateGC DECIMAL(38,21)      -- NUMERIC(38,21) NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'RFQCaseCode'. SQLSTATE: 42601 (line 1, pos 75)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `catalog`.`schema`.`RFQ` USING DELTA (     RFQCaseCode STRING,                     -- NVARCHAR(20) NOT NULL     RFQCaseName STRING,                     -- NVARCHAR(60) NOT NULL     RFQCaseLineNumber LONG,                 -- BIGINT NOT NULL     RFQCode STRING,                         -- NVARCHAR(20) NOT NULL     RFQName STRING,                          -- NVARCHAR(100) NOT NULL     RFQLineNumber DECIMAL(32,16),            -- NUMERIC(32,16) NOT NULL     SupplierCode STRING,                    -- NVARCHAR(20) NOT NULL     CompanyCode STRING,                     -- NVARCHAR(4) NOT NULL     ProductConfigurationCode STRING,        -- NVARCHAR(20) NOT NULL     ProductCode STRING,                     -- NVARCHAR(20) NOT NULL     DeliveryModeCode STRING,                -- NVARCHAR(10) NOT NULL     DeliveryTermsCode STRING,               -- NVARCHAR(10) NOT NULL     DefaultExchangeRateTypeCode STRING,      -- NVARCHAR(20) NOT NULL     BudgetExchangeRateTypeCode STRING,       -- NVARCHAR(20) NOT NULL     TransactionCurrencyCode STRING,         -- NVARCHAR(4) NOT NULL     AccountingCurrencyCode STRING,          -- NVARCHAR(3) NOT NULL     ReportingCurrencyCode STRING,            -- NVARCHAR(3) NOT NULL     GroupCurrencyCode STRING,               -- NVARCHAR(50) NOT NULL     DeliveryDate DATE,                      -- DATE NULL     ExpiryDate DATE,                        -- DATE NULL     CreatedDate DATE,                       -- DATE NULL     RFQCaseStatusLow STRING,                -- NVARCHAR(50) NULL     RFQCaseStatusHigh STRING,               -- NVARCHAR(50) NULL     RFQLineStatus STRING,                   -- NVARCHAR(50) NULL     PurchaseUnit STRING,                    -- NVARCHAR(10) NOT NULL     PurchQuantity DECIMAL(32,6),             -- NUMERIC(32,6) NOT NULL     PurchPriceTC DECIMAL(32,6),              -- NUMERIC(32,6) NOT NULL     PurchPriceAC DECIMAL(38,6),              -- NUMERIC(38,6) NOT NULL     PurchPriceRC DECIMAL(38,6),              -- NUMERIC(38,6) NOT NULL     PurchPriceGC DECIMAL(38,6),              -- NUMERIC(38,6) NOT NULL     PurchPriceAC_Budget DECIMAL(38,6),       -- NUMERIC(38,6) NOT NULL     PurchPriceRC_Budget DECIMAL(38,6),       -- NUMERIC(38,6) NOT NULL     PurchPriceGC_Budget DECIMAL(38,6),       -- NUMERIC(38,6) NOT NULL     RFQLineAmountTC DECIMAL(32,6),           -- NUMERIC(32,6) NOT NULL     RFQLineAmountAC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     RFQLineAmountRC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     RFQLineAmountGC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     RFQLineAmountAC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     RFQLineAmountRC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     RFQLineAmountGC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     CostAvoidanceAmountTC DECIMAL(33,6),     -- NUMERIC(33,6) NOT NULL     CostAvoidanceAmountAC DECIMAL(38,6),     -- NUMERIC(38,6) NOT NULL     CostAvoidanceAmountRC DECIMAL(38,6),     -- NUMERIC(38,6) NOT NULL     CostAvoidanceAmountGC DECIMAL(38,6),     -- NUMERIC(38,6) NOT NULL     CostAvoidanceAmountAC_Budget DECIMAL(38,6), -- NUMERIC(38,6) NOT NULL     CostAvoidanceAmountRC_Budget DECIMAL(38,6), -- NUMERIC(38,6) NOT NULL     CostAvoidanceAmountGC_Budget DECIMAL(38,6), -- NUMERIC(38,6) NOT NULL     MaxPurchPriceTC DECIMAL(32,6),           -- NUMERIC(32,6) NOT NULL     MaxPurchPriceAC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     MaxPurchPriceRC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     MaxPurchPriceGC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     MaxPurchPriceAC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     MaxPurchPriceRC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     MaxPurchPriceGC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     MinPurchPriceTC DECIMAL(32,6),           -- NUMERIC(32,6) NOT NULL     MinPurchPriceAC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     MinPurchPriceRC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     MinPurchPriceGC DECIMAL(38,6),           -- NUMERIC(38,6) NOT NULL     MinPurchPriceAC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     MinPurchPriceRC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     MinPurchPriceGC_Budget DECIMAL(38,6),    -- NUMERIC(38,6) NOT NULL     AppliedExchangeRateTC DECIMAL(38,6),      -- NUMERIC(38,6) NULL     AppliedExchangeRateRC DECIMAL(38,21),     -- NUMERIC(38,21) NOT NULL     AppliedExchangeRateAC DECIMAL(38,21),     -- NUMERIC(38,21) NOT NULL     AppliedExchangeRateGC DECIMAL(38,21)      -- NUMERIC(38,21) NOT NULL );
# MAGIC ---------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
