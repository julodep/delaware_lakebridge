# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_PurchaseBudget.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_PurchaseBudget.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks view creation – translation of the T‑SQL view
# ------------------------------------------------------------------
# The original T‑SQL view uses many legacy functions (ISNULL, DATEADD, EOMONTH, etc.)
# that have no direct counterparts in Spark SQL.  The table and view names are
# fully‑qualified with the target catalog `dbe_dbx_internships` and schema `datastore` as
# required.  Each reference such as DataStore.Date or dbo.SMRBIForecastSupplyForecastStaging
# has been rewritten to `dbe_dbx_internships.datastore.{table_name}` so that all objects live
# in the same Unity Catalog catalog and schema.
#
# Function mappings that were applied:
#   ISNULL  ->  COALESCE
#   NULLIF  ->  NULLIF
#   UPPER   ->  UPPER
#   EOMONTH ->  LAST_DAY
#   DATEADD (year)  ->  ADD_MONTHS( DATE_TRUNC('year', CURRENT_DATE()), +/- 24 )
#   GETDATE ->  CURRENT_DATE()
#   TIMESTAMP literals are avoided; we work with DATE or TIMESTAMP types directly.
#   CAST(x AS DECIMAL(p,s)) -> CAST(x AS DECIMAL(p,s))
#
# The view is created with "CREATE OR REPLACE VIEW" so that repeated
# executions overwrite the previous definition.
# ------------------------------------------------------------------

query = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_PurchaseBudget` AS
WITH DateRange AS (
    -- 1. Date range needed for joining with the forecast table
    SELECT DISTINCT
        LAST_DAY(D.`DateTime`) AS `BudgetDate`
    FROM `dbe_dbx_internships`.`datastore`.`Date` D
    WHERE D.`DateTime` >= (SELECT MIN(`StartDate`) 
                           FROM `dbe_dbx_internships`.`datastore`.`SMRBIForecastSupplyForecastStaging`)
      AND D.`DateTime` <= (SELECT MAX(`EndDate`) 
                           FROM `dbe_dbx_internships`.`datastore`.`SMRBIForecastSupplyForecastStaging`)
      -- 2. Range: two years before the start of the current year
      AND D.`DateTime` > ADD_MONTHS(DATE_TRUNC('year', CURRENT_DATE()), -24)
      -- .. and two years after the start of the current year
      AND D.`DateTime` < ADD_MONTHS(DATE_TRUNC('year', CURRENT_DATE()), 24)
)
SELECT
    -- 3. Normalise and null‑handle columns from the forecast table
    COALESCE(NULLIF(UPPER(FSFS.`ItemId`), ''), '_N/A')          AS `ProductCode`,
    COALESCE(NULLIF(FSFS.`ModelId`, ''), '_N/A')              AS `ForecastModelCode`,
    COALESCE(NULLIF(UPPER(FSFS.`DataAreaId`), ''), '_N/A')     AS `CompanyCode`,
    COALESCE(NULLIF(FSFS.`VendAccountId`, ''), '_N/A')         AS `SupplierCode`,
    COALESCE(NULLIF(L.`ExchangeRateType`, ''), '_N/A')         AS `DefaultExchangeRateTypeCode`,
    COALESCE(NULLIF(L.`BudgetExchangeRateType`, ''), '_N/A')   AS `BudgetExchangeRateTypeCode`,
    COALESCE(NULLIF(FSFS.`Currency`, ''), '_N/A')              AS `TransactionCurrencyCode`,
    COALESCE(NULLIF(L.`AccountingCurrency`, ''), '_N/A')       AS `AccountingCurrencyCode`,
    COALESCE(NULLIF(L.`ReportingCurrency`, ''), '_N/A')        AS `ReportingCurrencyCode`,
    COALESCE(NULLIF(L.`GroupCurrency`, ''), '_N/A')            AS `GroupCurrencyCode`,
    COALESCE(FSFS.`FORECASTSUPPLYFORECASTDIMENSION`, -1)      AS `DefaultDimension`,
    COALESCE(FSFS.`InventDimId`, '_N/A')                       AS `InventDimCode`,
    DR.`BudgetDate`,
    COALESCE(NULLIF(FSFS.`PurchUnitId`, ''), '_N/A')           AS `PurchaseUnit`,
    COALESCE(FSFS.`PurchQTY`, 0)                               AS `BudgetQuantity`,
    COALESCE(FSFS.`PurchPrice`, 0)                             AS `PurchUnitPriceTC`,
    -- 4. Exchange‑rate‑adjusted unit prices
    COALESCE(CASE WHEN FSFS.`Currency` = L.`AccountingCurrency`
                THEN FSFS.`PurchPrice` 
                ELSE FSFS.`PurchPrice` * AC.`ExchangeRate` END, 0) AS `PurchUnitPriceAC`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`ReportingCurrency`
                THEN FSFS.`PurchPrice` 
                ELSE FSFS.`PurchPrice` * RC.`ExchangeRate` END, 0) AS `PurchUnitPriceRC`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`GroupCurrency`
                THEN FSFS.`PurchPrice` 
                ELSE FSFS.`PurchPrice` * GC.`ExchangeRate` END, 0) AS `PurchUnitPriceGC`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`AccountingCurrency`
                THEN FSFS.`PurchPrice` 
                ELSE FSFS.`PurchPrice` * AC_Budget.`ExchangeRate` END, 0) AS `PurchUnitPriceAC_Budget`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`ReportingCurrency`
                THEN FSFS.`PurchPrice` 
                ELSE FSFS.`PurchPrice` * RC_Budget.`ExchangeRate` END, 0) AS `PurchUnitPriceRC_Budget`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`GroupCurrency`
                THEN FSFS.`PurchPrice` 
                ELSE FSFS.`PurchPrice` * GC_Budget.`ExchangeRate` END, 0) AS `PurchUnitPriceGC_Budget`,
    -- 5. Exchange‑rate‑adjusted amounts
    COALESCE(FSFS.`Amount`, 0)                                 AS `BudgetAmountTC`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`AccountingCurrency`
                THEN FSFS.`Amount` 
                ELSE FSFS.`Amount` * AC.`ExchangeRate` END, 0) AS `BudgetAmountAC`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`ReportingCurrency`
                THEN FSFS.`Amount` 
                ELSE FSFS.`Amount` * RC.`ExchangeRate` END, 0) AS `BudgetAmountRC`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`GroupCurrency`
                THEN FSFS.`Amount` 
                ELSE FSFS.`Amount` * GC.`ExchangeRate` END, 0) AS `BudgetAmountGC`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`AccountingCurrency`
                THEN FSFS.`Amount` 
                ELSE FSFS.`Amount` * AC_Budget.`ExchangeRate` END, 0) AS `BudgetAmountAC_Budget`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`ReportingCurrency`
                THEN FSFS.`Amount` 
                ELSE FSFS.`Amount` * RC_Budget.`ExchangeRate` END, 0) AS `BudgetAmountRC_Budget`,
    COALESCE(CASE WHEN FSFS.`Currency` = L.`GroupCurrency`
                THEN FSFS.`Amount` 
                ELSE FSFS.`Amount` * GC_Budget.`ExchangeRate` END, 0) AS `BudgetAmountGC_Budget`,
    CAST(1 AS DECIMAL(38,6))                                   AS `AppliedExchangeRateTC`,
    COALESCE(RC.`ExchangeRate`, 0)                             AS `AppliedExchangeRateRC`,
    COALESCE(AC.`ExchangeRate`, 0)                             AS `AppliedExchangeRateAC`,
    COALESCE(GC.`ExchangeRate`, 0)                             AS `AppliedExchangeRateGC`,
    COALESCE(RC_Budget.`ExchangeRate`, 0)                       AS `AppliedExchangeRateRC_Budget`,
    COALESCE(AC_Budget.`ExchangeRate`, 0)                       AS `AppliedExchangeRateAC_Budget`,
    COALESCE(GC_Budget.`ExchangeRate`, 0)                       AS `AppliedExchangeRateGC_Budget`
FROM DateRange DR
JOIN `dbe_dbx_internships`.`datastore`.`SMRBIForecastSupplyForecastStaging` FSFS
  ON DR.`BudgetDate` >= FSFS.`StartDate` AND DR.`BudgetDate` <= FSFS.`EndDate`
JOIN (
        SELECT DISTINCT 
                LES.`ReportingCurrency`,
                LES.`AccountingCurrency`,
                LES.`ExchangeRateType`,
                LES.`BudgetExchangeRateType`,
                LES.`Name`,
                G.`GroupCurrencyCode` AS `GroupCurrency`
        FROM `dbe_dbx_internships`.`datastore`.`SMRBILedgerStaging` LES
        CROSS JOIN (SELECT `GroupCurrencyCode` FROM `dbe_dbx_internships`.`datastore`.`GroupCurrency` LIMIT 1) G
     ) L
  ON FSFS.`DataAreaId` = L.`Name`
LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` RC
  ON RC.`FromCurrencyCode` = FSFS.`Currency`
 AND RC.`ToCurrencyCode` = L.`ReportingCurrency`
 AND RC.`ExchangeRateTypeCode` = L.`ExchangeRateType`
 AND FSFS.`StartDate` BETWEEN RC.`ValidFrom` AND RC.`ValidTo`
LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` AC
  ON AC.`FromCurrencyCode` = FSFS.`Currency`
 AND AC.`ToCurrencyCode` = L.`AccountingCurrency`
 AND AC.`ExchangeRateTypeCode` = L.`ExchangeRateType`
 AND FSFS.`StartDate` BETWEEN AC.`ValidFrom` AND AC.`ValidTo`
LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` GC
  ON GC.`FromCurrencyCode` = FSFS.`Currency`
 AND GC.`ToCurrencyCode` = L.`GroupCurrency`
 AND GC.`ExchangeRateTypeCode` = L.`ExchangeRateType`
 AND FSFS.`StartDate` BETWEEN GC.`ValidFrom` AND GC.`ValidTo`
LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` RC_Budget
  ON RC_Budget.`FromCurrencyCode` = FSFS.`Currency`
 AND RC_Budget.`ToCurrencyCode` = L.`ReportingCurrency`
 AND RC_Budget.`ExchangeRateTypeCode` = L.`BudgetExchangeRateType`
 AND FSFS.`StartDate` BETWEEN RC_Budget.`ValidFrom` AND RC_Budget.`ValidTo`
LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` AC_Budget
  ON AC_Budget.`FromCurrencyCode` = FSFS.`Currency`
 AND AC_Budget.`ToCurrencyCode` = L.`AccountingCurrency`
 AND AC_Budget.`ExchangeRateTypeCode` = L.`BudgetExchangeRateType`
 AND FSFS.`StartDate` BETWEEN AC_Budget.`ValidFrom` AND AC_Budget.`ValidTo`
LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` GC_Budget
  ON GC_Budget.`FromCurrencyCode` = FSFS.`Currency`
 AND GC_Budget.`ToCurrencyCode` = L.`GroupCurrency`
 AND GC_Budget.`ExchangeRateTypeCode` = L.`BudgetExchangeRateType`
  AND FSFS.`StartDate` BETWEEN GC_Budget.`ValidFrom` AND GC_Budget.`ValidTo`;
"""

# COMMAND ----------

# Execute the DDL statement
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 7881)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_PurchaseBudget` AS WITH DateRange AS (     -- 1. Date range needed for joining with the forecast table     SELECT DISTINCT         LAST_DAY(D.`DateTime`) AS `BudgetDate`     FROM `_placeholder_`.`_placeholder_`.`Date` D     WHERE D.`DateTime` >= (SELECT MIN(`StartDate`)                             FROM `_placeholder_`.`_placeholder_`.`SMRBIForecastSupplyForecastStaging`)       AND D.`DateTime` <= (SELECT MAX(`EndDate`)                             FROM `_placeholder_`.`_placeholder_`.`SMRBIForecastSupplyForecastStaging`)       -- 2. Range: two years before the start of the current year       AND D.`DateTime` > ADD_MONTHS(DATE_TRUNC('year', CURRENT_DATE()), -24)       -- .. and two years after the start of the current year       AND D.`DateTime` < ADD_MONTHS(DATE_TRUNC('year', CURRENT_DATE()), 24) ) SELECT     -- 3. Normalise and null‑handle columns from the forecast table     COALESCE(NULLIF(UPPER(FSFS.`ItemId`), ''), '_N/A')          AS `ProductCode`,     COALESCE(NULLIF(FSFS.`ModelId`, ''), '_N/A')              AS `ForecastModelCode`,     COALESCE(NULLIF(UPPER(FSFS.`DataAreaId`), ''), '_N/A')     AS `CompanyCode`,     COALESCE(NULLIF(FSFS.`VendAccountId`, ''), '_N/A')         AS `SupplierCode`,     COALESCE(NULLIF(L.`ExchangeRateType`, ''), '_N/A')         AS `DefaultExchangeRateTypeCode`,     COALESCE(NULLIF(L.`BudgetExchangeRateType`, ''), '_N/A')   AS `BudgetExchangeRateTypeCode`,     COALESCE(NULLIF(FSFS.`Currency`, ''), '_N/A')              AS `TransactionCurrencyCode`,     COALESCE(NULLIF(L.`AccountingCurrency`, ''), '_N/A')       AS `AccountingCurrencyCode`,     COALESCE(NULLIF(L.`ReportingCurrency`, ''), '_N/A')        AS `ReportingCurrencyCode`,     COALESCE(NULLIF(L.`GroupCurrency`, ''), '_N/A')            AS `GroupCurrencyCode`,     COALESCE(FSFS.`FORECASTSUPPLYFORECASTDIMENSION`, -1)      AS `DefaultDimension`,     COALESCE(FSFS.`InventDimId`, '_N/A')                       AS `InventDimCode`,     DR.`BudgetDate`,     COALESCE(NULLIF(FSFS.`PurchUnitId`, ''), '_N/A')           AS `PurchaseUnit`,     COALESCE(FSFS.`PurchQTY`, 0)                               AS `BudgetQuantity`,     COALESCE(FSFS.`PurchPrice`, 0)                             AS `PurchUnitPriceTC`,     -- 4. Exchange‑rate‑adjusted unit prices     COALESCE(CASE WHEN FSFS.`Currency` = L.`AccountingCurrency`                 THEN FSFS.`PurchPrice`                  ELSE FSFS.`PurchPrice` * AC.`ExchangeRate` END, 0) AS `PurchUnitPriceAC`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`ReportingCurrency`                 THEN FSFS.`PurchPrice`                  ELSE FSFS.`PurchPrice` * RC.`ExchangeRate` END, 0) AS `PurchUnitPriceRC`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`GroupCurrency`                 THEN FSFS.`PurchPrice`                  ELSE FSFS.`PurchPrice` * GC.`ExchangeRate` END, 0) AS `PurchUnitPriceGC`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`AccountingCurrency`                 THEN FSFS.`PurchPrice`                  ELSE FSFS.`PurchPrice` * AC_Budget.`ExchangeRate` END, 0) AS `PurchUnitPriceAC_Budget`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`ReportingCurrency`                 THEN FSFS.`PurchPrice`                  ELSE FSFS.`PurchPrice` * RC_Budget.`ExchangeRate` END, 0) AS `PurchUnitPriceRC_Budget`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`GroupCurrency`                 THEN FSFS.`PurchPrice`                  ELSE FSFS.`PurchPrice` * GC_Budget.`ExchangeRate` END, 0) AS `PurchUnitPriceGC_Budget`,     -- 5. Exchange‑rate‑adjusted amounts     COALESCE(FSFS.`Amount`, 0)                                 AS `BudgetAmountTC`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`AccountingCurrency`                 THEN FSFS.`Amount`                  ELSE FSFS.`Amount` * AC.`ExchangeRate` END, 0) AS `BudgetAmountAC`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`ReportingCurrency`                 THEN FSFS.`Amount`                  ELSE FSFS.`Amount` * RC.`ExchangeRate` END, 0) AS `BudgetAmountRC`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`GroupCurrency`                 THEN FSFS.`Amount`                  ELSE FSFS.`Amount` * GC.`ExchangeRate` END, 0) AS `BudgetAmountGC`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`AccountingCurrency`                 THEN FSFS.`Amount`                  ELSE FSFS.`Amount` * AC_Budget.`ExchangeRate` END, 0) AS `BudgetAmountAC_Budget`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`ReportingCurrency`                 THEN FSFS.`Amount`                  ELSE FSFS.`Amount` * RC_Budget.`ExchangeRate` END, 0) AS `BudgetAmountRC_Budget`,     COALESCE(CASE WHEN FSFS.`Currency` = L.`GroupCurrency`                 THEN FSFS.`Amount`                  ELSE FSFS.`Amount` * GC_Budget.`ExchangeRate` END, 0) AS `BudgetAmountGC_Budget`,     CAST(1 AS DECIMAL(38,6))                                   AS `AppliedExchangeRateTC`,     COALESCE(RC.`ExchangeRate`, 0)                             AS `AppliedExchangeRateRC`,     COALESCE(AC.`ExchangeRate`, 0)                             AS `AppliedExchangeRateAC`,     COALESCE(GC.`ExchangeRate`, 0)                             AS `AppliedExchangeRateGC`,     COALESCE(RC_Budget.`ExchangeRate`, 0)                       AS `AppliedExchangeRateRC_Budget`,     COALESCE(AC_Budget.`ExchangeRate`, 0)                       AS `AppliedExchangeRateAC_Budget`,     COALESCE(GC_Budget.`ExchangeRate`, 0)                       AS `AppliedExchangeRateGC_Budget` FROM DateRange DR JOIN `_placeholder_`.`_placeholder_`.`SMRBIForecastSupplyForecastStaging` FSFS   ON DR.`BudgetDate` >= FSFS.`StartDate` AND DR.`BudgetDate` <= FSFS.`EndDate` JOIN (         SELECT DISTINCT                  LES.`ReportingCurrency`,                 LES.`AccountingCurrency`,                 LES.`ExchangeRateType`,                 LES.`BudgetExchangeRateType`,                 LES.`Name`,                 G.`GroupCurrencyCode` AS `GroupCurrency`         FROM `_placeholder_`.`_placeholder_`.`SMRBILedgerStaging` LES         CROSS JOIN (SELECT `GroupCurrencyCode` FROM `_placeholder_`.`_placeholder_`.`GroupCurrency` LIMIT 1) G      ) L   ON FSFS.`DataAreaId` = L.`Name` LEFT JOIN `_placeholder_`.`_placeholder_`.`ExchangeRate` RC   ON RC.`FromCurrencyCode` = FSFS.`Currency`  AND RC.`ToCurrencyCode` = L.`ReportingCurrency`  AND RC.`ExchangeRateTypeCode` = L.`ExchangeRateType`  AND FSFS.`StartDate` BETWEEN RC.`ValidFrom` AND RC.`ValidTo` LEFT JOIN `_placeholder_`.`_placeholder_`.`ExchangeRate` AC   ON AC.`FromCurrencyCode` = FSFS.`Currency`  AND AC.`ToCurrencyCode` = L.`AccountingCurrency`  AND AC.`ExchangeRateTypeCode` = L.`ExchangeRateType`  AND FSFS.`StartDate` BETWEEN AC.`ValidFrom` AND AC.`ValidTo` LEFT JOIN `_placeholder_`.`_placeholder_`.`ExchangeRate` GC   ON GC.`FromCurrencyCode` = FSFS.`Currency`  AND GC.`ToCurrencyCode` = L.`GroupCurrency`  AND GC.`ExchangeRateTypeCode` = L.`ExchangeRateType`  AND FSFS.`StartDate` BETWEEN GC.`ValidFrom` AND GC.`ValidTo` LEFT JOIN `_placeholder_`.`_placeholder_`.`ExchangeRate` RC_Budget   ON RC_Budget.`FromCurrencyCode` = FSFS.`Currency`  AND RC_Budget.`ToCurrencyCode` = L.`ReportingCurrency`  AND RC_Budget.`ExchangeRateTypeCode` = L.`BudgetExchangeRateType`  AND FSFS.`StartDate` BETWEEN RC_Budget.`ValidFrom` AND RC_Budget.`ValidTo` LEFT JOIN `_placeholder_`.`_placeholder_`.`ExchangeRate` AC_Budget   ON AC_Budget.`FromCurrencyCode` = FSFS.`Currency`  AND AC_Budget.`ToCurrencyCode` = L.`AccountingCurrency`  AND AC_Budget.`ExchangeRateTypeCode` = L.`BudgetExchangeRateType`  AND FSFS.`StartDate` BETWEEN AC_Budget.`ValidFrom` AND AC_Budget.`ValidTo` LEFT JOIN `_placeholder_`.`_placeholder_`.`ExchangeRate` GC_Budget   ON GC_Budget.`FromCurrencyCode` = FSFS.`Currency`  AND GC_Budget.`ToCurrencyCode` = L.`GroupCurrency`  AND GC_Budget.`ExchangeRateTypeCode` = L.`BudgetExchangeRateType`   AND FSFS.`StartDate` BETWEEN GC_Budget.`ValidFrom` AND GC_Budget.`ValidTo`;
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
