# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesBudget.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.SalesBudget.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# 1️⃣  Define the target catalogue and schema
#     (replace the placeholders with your actual names)
# --------------------------------------------------------------
catalog = "dbe_dbx_internships"          # e.g., "my_catalog"
schema  = "datastore"           # e.g., "my_schema"

# COMMAND ----------

# --------------------------------------------------------------
# 2️⃣  Create the "SalesBudget" table
#     --------------------------------------------------------------
#     • All database object references are fully-qualified:
#           `dbe_dbx_internships`.`datastore`.`SalesBudget`
#     • T‑SQL datatype conversions are mapped to Spark SQL equivalents:
#           numeric(p,s) → DECIMAL(p,s)
#           nvarchar(*) → STRING
#           datetime   → TIMESTAMP
#           bigint     → BIGINT (Spark calls it LONG but it is the same)
#     • The table is created as a Delta table – the default format
#       in Unity Catalog – so that it can be managed and queried
#       consistently.
# --------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`datastore`.`SalesBudget` (
  -- 1️⃣  Simple string columns
  Comment                     STRING NOT NULL,
  CompanyCode                 STRING,
  ProductCode                 STRING NOT NULL,
  ProductGroupCode            STRING NOT NULL,
  CustomerCode                STRING NOT NULL,
  CustomerGroupCode           STRING NOT NULL,
  ForecastModelCode           STRING NOT NULL,
  InventDimCode               STRING NOT NULL,
  DefaultExchangeRateTypeCode STRING NOT NULL,
  BudgetExchangeRateTypeCode  STRING NOT NULL,
  TransactionCurrencyCode     STRING,
  AccountingCurrencyCode      STRING NOT NULL,
  ReportingCurrencyCode       STRING NOT NULL,
  GroupCurrencyCode           STRING NOT NULL,

  -- 2️⃣  Timestamp / numeric columns
  ForecastDate                TIMESTAMP NOT NULL,
  SalesUnit                   STRING NOT NULL,

  ForecastQuantity            DECIMAL(32,6) NOT NULL,
  GrossSalesAmountTC          DECIMAL(32,6) NOT NULL,
  GrossSalesAmountAC          DECIMAL(38,6) NOT NULL,
  GrossSalesAmountRC          DECIMAL(38,6) NOT NULL,
  GrossSalesAmountGC          DECIMAL(38,6) NOT NULL,
  GrossSalesAmountAC_Budget   DECIMAL(38,6) NOT NULL,
  GrossSalesAmountRC_Budget   DECIMAL(38,6) NOT NULL,
  GrossSalesAmountGC_Budget   DECIMAL(38,6) NOT NULL,

  CostPriceTC                 DECIMAL(38,6) NOT NULL,
  CostPriceAC                 DECIMAL(38,6) NOT NULL,
  CostPriceRC                 DECIMAL(38,6) NOT NULL,
  CostPriceGC                 DECIMAL(38,6) NOT NULL,
  CostPriceAC_Budget          DECIMAL(38,6) NOT NULL,
  CostPriceRC_Budget          DECIMAL(38,6) NOT NULL,
  CostPriceGC_Budget          DECIMAL(38,6) NOT NULL,

  GrossMarginTC               DECIMAL(38,6),          -- optional (nullable)
  GrossMarginAC               DECIMAL(38,6) NOT NULL,
  GrossMarginRC               DECIMAL(38,6) NOT NULL,
  GrossMarginGC               DECIMAL(38,6) NOT NULL,
  GrossMarginAC_Budget        DECIMAL(38,6) NOT NULL,
  GrossMarginRC_Budget        DECIMAL(38,6) NOT NULL,
  GrossMarginGC_Budget        DECIMAL(38,6) NOT NULL,

  AppliedExchangeRateTC       DECIMAL(38,6),          -- optional (nullable)
  AppliedExchangeRateRC       DECIMAL(38,21) NOT NULL,
  AppliedExchangeRateAC       DECIMAL(38,21) NOT NULL,
  AppliedExchangeRateGC       DECIMAL(38,21) NOT NULL,
  AppliedExchangeRateRC_Budget DECIMAL(38,21) NOT NULL,
  AppliedExchangeRateAC_Budget DECIMAL(38,21) NOT NULL,
  AppliedExchangeRateGC_Budget DECIMAL(38,21) NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# --------------------------------------------------------------
# 3️⃣  Verify creation (optional)
#     We just read back the metadata of the table to ensure it
#     was created correctly.  The `table` command is a handy
#     Spark SQL tool for this.
# --------------------------------------------------------------
spark.sql(f"DESC TABLE `dbe_dbx_internships`.`datastore`.`SalesBudget`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2515)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`SalesBudget` (   -- 1️⃣  Simple string columns   Comment                     STRING NOT NULL,   CompanyCode                 STRING,   ProductCode                 STRING NOT NULL,   ProductGroupCode            STRING NOT NULL,   CustomerCode                STRING NOT NULL,   CustomerGroupCode           STRING NOT NULL,   ForecastModelCode           STRING NOT NULL,   InventDimCode               STRING NOT NULL,   DefaultExchangeRateTypeCode STRING NOT NULL,   BudgetExchangeRateTypeCode  STRING NOT NULL,   TransactionCurrencyCode     STRING,   AccountingCurrencyCode      STRING NOT NULL,   ReportingCurrencyCode       STRING NOT NULL,   GroupCurrencyCode           STRING NOT NULL,    -- 2️⃣  Timestamp / numeric columns   ForecastDate                TIMESTAMP NOT NULL,   SalesUnit                   STRING NOT NULL,    ForecastQuantity            DECIMAL(32,6) NOT NULL,   GrossSalesAmountTC          DECIMAL(32,6) NOT NULL,   GrossSalesAmountAC          DECIMAL(38,6) NOT NULL,   GrossSalesAmountRC          DECIMAL(38,6) NOT NULL,   GrossSalesAmountGC          DECIMAL(38,6) NOT NULL,   GrossSalesAmountAC_Budget   DECIMAL(38,6) NOT NULL,   GrossSalesAmountRC_Budget   DECIMAL(38,6) NOT NULL,   GrossSalesAmountGC_Budget   DECIMAL(38,6) NOT NULL,    CostPriceTC                 DECIMAL(38,6) NOT NULL,   CostPriceAC                 DECIMAL(38,6) NOT NULL,   CostPriceRC                 DECIMAL(38,6) NOT NULL,   CostPriceGC                 DECIMAL(38,6) NOT NULL,   CostPriceAC_Budget          DECIMAL(38,6) NOT NULL,   CostPriceRC_Budget          DECIMAL(38,6) NOT NULL,   CostPriceGC_Budget          DECIMAL(38,6) NOT NULL,    GrossMarginTC               DECIMAL(38,6),          -- optional (nullable)   GrossMarginAC               DECIMAL(38,6) NOT NULL,   GrossMarginRC               DECIMAL(38,6) NOT NULL,   GrossMarginGC               DECIMAL(38,6) NOT NULL,   GrossMarginAC_Budget        DECIMAL(38,6) NOT NULL,   GrossMarginRC_Budget        DECIMAL(38,6) NOT NULL,   GrossMarginGC_Budget        DECIMAL(38,6) NOT NULL,    AppliedExchangeRateTC       DECIMAL(38,6),          -- optional (nullable)   AppliedExchangeRateRC       DECIMAL(38,21) NOT NULL,   AppliedExchangeRateAC       DECIMAL(38,21) NOT NULL,   AppliedExchangeRateGC       DECIMAL(38,21) NOT NULL,   AppliedExchangeRateRC_Budget DECIMAL(38,21) NOT NULL,   AppliedExchangeRateAC_Budget DECIMAL(38,21) NOT NULL,   AppliedExchangeRateGC_Budget DECIMAL(38,21) NOT NULL ) USING DELTA
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
