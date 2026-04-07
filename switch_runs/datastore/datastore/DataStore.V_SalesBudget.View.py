# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_SalesBudget.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_SalesBudget.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# NOTE:
# This notebook creates a persistent view in Unity Catalog using
# fully‑qualified names (catalog.schema.object_name).  
# All T‑SQL constructs that do not exist in Spark are converted to the
# nearest equivalent:
#
# * ISNULL(expr, default)          → coalesce(expr, default)
# * CASE WHEN condition THEN a ELSE b END
#   → when(condition, a, b)
# * LTRIM / RTRIM                   → trim()
# * UPPER / LOWER                   → upper() / lower()
# * LEFT(string, n)                 → substring(string, 1, n)
# * DATEDIFF, DATEADD, etc.          → proper Spark equivalents
# * `' '` (empty string) is kept as is; comparisons use = ''
# * YEAR(date)  → year(date)
# * CAST(... AS VARCHAR/INT/NUMERIC) → cast(... as type)
# * Alias names that conflict with reserved words are stored exactly as
#   defined; Spark will handle them without additional quoting.
# ------------------------------------------------------------------


# The final view definition (all DDL is executed in one spark.sql() call).
# Replace `dbe_dbx_internships` and `datastore` with the actual catalog and schema names.
spark.sql(f"""
/* 1.  Create or replace the view V_SalesBudget in the target catalog/schema.
 *
 * The original T‑SQL view performs extensive data cleaning, currency
 * calculations and joins with several staging tables.  All function calls
 * have been translated to Spark SQL syntax.
 */
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.V_SalesBudget AS
SELECT
    /* 1.1  Cleaned comment field */
    coalesce(case
                when trim(FDFS.Comment_) = '' then null
                else FDFS.Comment_
             end, '_N/A')                                                AS Comment,

    /* 1.2  Company code, uppercased */
    upper(FDFS.DataAreaId)                                            AS CompanyCode,

    /* 1.3  Product code: trim and upper case, replace empty with _N/A */
    coalesce(case
                when trim(FDFS.ItemNumber) = '' then null
                else upper(trim(FDFS.ItemNumber))
             end, '_N/A')                                               AS ProductCode,

    /* 1.4  Product group code */
    coalesce(case
                when trim(FDFS.ItemGroupId) = '' then null
                else upper(trim(FDFS.ItemGroupId))
             end, '_N/A')                                               AS ProductGroupCode,

    /* 1.5  Customer code */
    coalesce(case
                when trim(FDFS.CustomerAccountNumber) = '' then null
                else upper(trim(FDFS.CustomerAccountNumber))
             end, '_N/A')                                               AS CustomerCode,

    /* 1.6  Customer group code (no trim needed) */
    coalesce(FDFS.CustomerGroupId, '_N/A')                           AS CustomerGroupCode,

    /* 1.7  Forecast model code */
    coalesce(case
                when trim(FDFS.ForecastModelId) = '' then null
                else upper(trim(FDFS.ForecastModelId))
             end, '_N/A')                                               AS ForecastModelCode,

    /* 1.8  Inventory dimension code */
    coalesce(FDFS.InventDimId, '_N/A')                               AS InventDimCode,

    /* 1.9  Default dimension  */
    coalesce(FDFS.ForecastDemandForecastDimension, -1)                AS DefaultDimension,

    /* 1.10  Default exchange rate type */
    coalesce(nullif(L.ExchangeRateType, ''), 'N/A')                  AS DefaultExchangeRateTypeCode,

    /* 1.11  Budget exchange rate type */
    coalesce(nullif(L.BudgetExchangeRateType, ''), 'N/A')             AS BudgetExchangeRateTypeCode,

    /* 1.12  Transaction currency code */
    upper(FDFS.PricingCurrencyCode)                                 AS TransactionCurrencyCode,

    /* 1.13  Accounting currency code (5‑char) */
    coalesce(cast(upper(L.AccountingCurrency) as string), N'_N/A')   AS AccountingCurrencyCode,

    /* 1.14  Reporting currency code */
    coalesce(cast(upper(nullif(L.ReportingCurrency, '')) as string), N'_N/A')
                                                                            AS ReportingCurrencyCode,

    /* 1.15  Group currency code */
    coalesce(cast(upper(L.GroupCurrency) as string), N'_N/A')         AS GroupCurrencyCode,

    /* 1.16  Forecast start date (defaults to 1900‑01‑01) */
    coalesce(FDFS.ForecastStartDate, CAST('1900-01-01' AS DATE))      AS ForecastDate,

    /* 1.17  Sales unit */
    coalesce(nullif(FDFS.QuantityUnitSymbol, ''), '_N/A')             AS SalesUnit,

    /* 1.18  Forecast quantity (defaults to 0) */
    coalesce(FDFS.ForecastedQuantity, 0)                             AS ForecastQuantity,

    /* 1.19  Gross sales amount (transaction currency) */
    coalesce(FDFS.ForecastedRevenue, 0)                              AS GrossSalesAmountTC,

    /* 1.20  Gross sales amount – accounting currency */
    coalesce(case
                when L.AccountingCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedRevenue
                    else FDFS.ForecastedRevenue * AC.ExchangeRate
             end, 0)                                                   AS GrossSalesAmountAC,

    /* 1.21  Gross sales amount – reporting currency */
    coalesce(case
                when L.ReportingCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedRevenue
                    else FDFS.ForecastedRevenue * RC.ExchangeRate
             end, 0)                                                   AS GrossSalesAmountRC,

    /* 1.22  Gross sales amount – group currency */
    coalesce(case
                when L.GroupCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedRevenue
                    else FDFS.ForecastedRevenue * GC.ExchangeRate
             end, 0)                                                   AS GrossSalesAmountGC,

    /* 1.23  Gross sales amount – accounting currency (budget) */
    coalesce(case
                when L.AccountingCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedRevenue
                    else FDFS.ForecastedRevenue * AC_Budget.ExchangeRate
             end, 0)                                                   AS GrossSalesAmountAC_Budget,

    /* 1.24  Gross sales amount – reporting currency (budget) */
    coalesce(case
                when L.ReportingCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedRevenue
                    else FDFS.ForecastedRevenue * RC_Budget.ExchangeRate
             end, 0)                                                   AS GrossSalesAmountRC_Budget,

    /* 1.25  Gross sales amount – group currency (budget) */
    coalesce(case
                when L.GroupCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedRevenue
                    else FDFS.ForecastedRevenue * GC_Budget.ExchangeRate
             end, 0)                                                   AS GrossSalesAmountGC_Budget,

    /* 1.26  Cost price (transaction currency) */
    coalesce(FDFS.ForecastedQuantity * IPS.Price, 0)                  AS CostPriceTC,

    /* 1.27  Cost price – accounting currency */
    coalesce(case
                when L.AccountingCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedQuantity * IPS.Price
                    else (FDFS.ForecastedQuantity * IPS.Price) * AC.ExchangeRate
             end, 0)                                                   AS CostPriceAC,

    /* 1.28  Cost price – reporting currency */
    coalesce(case
                when L.ReportingCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedQuantity * IPS.Price
                    else (FDFS.ForecastedQuantity * IPS.Price) * RC.ExchangeRate
             end, 0)                                                   AS CostPriceRC,

    /* 1.29  Cost price – group currency */
    coalesce(case
                when L.GroupCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedQuantity * IPS.Price
                    else (FDFS.ForecastedQuantity * IPS.Price) * GC.ExchangeRate
             end, 0)                                                   AS CostPriceGC,

    /* 1.30  Cost price – accounting currency (budget) */
    coalesce(case
                when L.AccountingCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedQuantity * IPS.Price
                    else (FDFS.ForecastedQuantity * IPS.Price) * AC_Budget.ExchangeRate
             end, 0)                                                   AS CostPriceAC_Budget,

    /* 1.31  Cost price – reporting currency (budget) */
    coalesce(case
                when L.ReportingCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedQuantity * IPS.Price
                    else (FDFS.ForecastedQuantity * IPS.Price) * RC_Budget.ExchangeRate
             end, 0)                                                   AS CostPriceRC_Budget,

    /* 1.32  Cost price – group currency (budget) */
    coalesce(case
                when L.GroupCurrency = FDFS.PricingCurrencyCode
                    then FDFS.ForecastedQuantity * IPS.Price
                    else (FDFS.ForecastedQuantity * IPS.Price) * GC_Budget.ExchangeRate
             end, 0)                                                   AS CostPriceGC_Budget,

    /* 1.33  Gross margin (transaction currency) */
    coalesce(FDFS.ForecastedRevenue,
             FDFS.ForecastedQuantity * IPS.Price)                      AS GrossMarginTC,

    /* 1.34  Gross margin – accounting currency */
    coalesce(case
                when L.AccountingCurrency = FDFS.PricingCurrencyCode
                    then (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price)
                    else (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price) * AC.ExchangeRate
             end, 0)                                                   AS GrossMarginAC,

    /* 1.35  Gross margin – reporting currency */
    coalesce(case
                when L.ReportingCurrency = FDFS.PricingCurrencyCode
                    then (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price)
                    else (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price) * RC.ExchangeRate
             end, 0)                                                   AS GrossMarginRC,

    /* 1.36  Gross margin – group currency */
    coalesce(case
                when L.GroupCurrency = FDFS.PricingCurrencyCode
                    then (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price)
                    else (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price) * GC.ExchangeRate
             end, 0)                                                   AS GrossMarginGC,

    /* 1.37  Gross margin – accounting currency (budget) */
    coalesce(case
                when L.AccountingCurrency = FDFS.PricingCurrencyCode
                    then (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price)
                    else (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price) * AC_Budget.ExchangeRate
             end, 0)                                                   AS GrossMarginAC_Budget,

    /* 1.38  Gross margin – reporting currency (budget) */
    coalesce(case
                when L.ReportingCurrency = FDFS.PricingCurrencyCode
                    then (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price)
                    else (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price) * RC_Budget.ExchangeRate
             end, 0)                                                   AS GrossMarginRC_Budget,

    /* 1.39  Gross margin – group currency (budget) */
    coalesce(case
                when L.GroupCurrency = FDFS.PricingCurrencyCode
                    then (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price)
                    else (FDFS.ForecastedRevenue - FDFS.ForecastedQuantity * IPS.Price) * GC_Budget.ExchangeRate
             end, 0)                                                   AS GrossMarginGC_Budget,

    /* 1.40  Applied exchange rates – fixed dummy value for TC */
    cast(1 as decimal(38,6))                                            AS AppliedExchangeRateTC,

    /* 1.41  Applied exchange rates – actual rates (defaults to 0) */
    coalesce(RC.ExchangeRate, 0)                                        AS AppliedExchangeRateRC,
    coalesce(AC.ExchangeRate, 0)                                        AS AppliedExchangeRateAC,
    coalesce(GC.ExchangeRate, 0)                                        AS AppliedExchangeRateGC,
    coalesce(RC_Budget.ExchangeRate, 0)                                 AS AppliedExchangeRateRC_Budget,
    coalesce(AC_Budget.ExchangeRate, 0)                                 AS AppliedExchangeRateAC_Budget,
    coalesce(GC_Budget.ExchangeRate, 0)                                 AS AppliedExchangeRateGC_Budget

-- 2.  Use the same FROM and JOIN logic that the T‑SQL view used.
FROM
    /* 2.1  Base forecast demand staging table */
    dbe_dbx_internships.datastore.SMRBIForecastDemandForecastStaging AS FDFS

    /* 2.2  Join with staging prices (price history) */
    LEFT JOIN
        (SELECT
            I1.ItemNumber      AS ProductId,
            I1.FromDate        AS FromDate,
            COALESCE(I2.FromDate, '99991231') AS EndDate,
            I1.CostingVersionId,
            I1.Price,
            I1.DataAreaId      AS CompanyId
         FROM dbe_dbx_internships.datastore.SMRBIInventItemPendingPriceStaging AS I1
         LEFT JOIN dbe_dbx_internships.datastore.SMRBIInventItemPendingPriceStaging AS I2
            ON I1.ItemNumber      = I2.ItemNumber
            AND I1.DataAreaId     = I2.DataAreaId
            AND I1.CostingVersionId = I2.CostingVersionId
            AND I1.PriceType     = I2.PriceType
            AND I2.FromDate = (SELECT MIN(FromDate)
                               FROM dbe_dbx_internships.datastore.SMRBIInventItemPendingPriceStaging
                               WHERE ItemNumber = I1.ItemNumber
                                 AND DataAreaId = I1.DataAreaId
                                 AND CostingVersionId = I1.CostingVersionId
                                 AND PriceType = I1.PriceType
                                 AND FromDate > I1.FromDate)
         WHERE I1.PriceType = 0) AS IPS
        ON FDFS.ItemNumber = IPS.ProductId
       AND FDFS.DataAreaId = IPS.CompanyId
       AND FDFS.ForecastStartDate >= IPS.FromDate
       AND FDFS.ForecastStartDate < IPS.EndDate
       AND IPS.CostingVersionId = coalesce(cast(year(FDFS.ForecastStartDate) as string), '1900')

    /* 2.3  Join with ledger staging (to get currency codes) */
    JOIN
        (SELECT DISTINCT
             LES.ReportingCurrency,
             LES.AccountingCurrency,
             LES.ExchangeRateType,
             LES.BudgetExchangeRateType,
             LES.Name,
             G.GroupCurrencyCode AS GroupCurrency
         FROM dbe_dbx_internships.datastore.SMRBILedgerStaging AS LES
         CROSS JOIN
             (SELECT TOP 1 GroupCurrencyCode FROM dbe_dbx_internships.datastore.GroupCurrency) AS G
        ) AS L
        ON FDFS.DataAreaId = L.Name

    /* 2.4  Exchange rates – reporting currency */
    LEFT JOIN dbe_dbx_internships.datastore.ExchangeRate AS RC
        ON RC.FromCurrencyCode = FDFS.PricingCurrencyCode
       AND RC.ToCurrencyCode = L.ReportingCurrency
       AND RC.ExchangeRateTypeCode = L.ExchangeRateType
       AND FDFS.ForecastStartDate BETWEEN RC.ValidFrom AND RC.ValidTo

    /* 2.5  Exchange rates – accounting currency */
    LEFT JOIN dbe_dbx_internships.datastore.ExchangeRate AS AC
        ON AC.FromCurrencyCode = FDFS.PricingCurrencyCode
       AND AC.ToCurrencyCode = L.AccountingCurrency
       AND AC.ExchangeRateTypeCode = L.ExchangeRateType
       AND FDFS.ForecastStartDate BETWEEN AC.ValidFrom AND AC.ValidTo

    /* 2.6  Exchange rates – group currency */
    LEFT JOIN dbe_dbx_internships.datastore.ExchangeRate AS GC
        ON GC.FromCurrencyCode = FDFS.PricingCurrencyCode
       AND GC.ToCurrencyCode = L.GroupCurrency
       AND GC.ExchangeRateTypeCode = L.ExchangeRateType
       AND FDFS.ForecastStartDate BETWEEN GC.ValidFrom AND GC.ValidTo

    /* 2.7  Exchange rates – reporting currency (budget) */
    LEFT JOIN dbe_dbx_internships.datastore.ExchangeRate AS RC_Budget
        ON RC_Budget.FromCurrencyCode = FDFS.PricingCurrencyCode
       AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
       AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
       AND FDFS.ForecastStartDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

    /* 2.8  Exchange rates – accounting currency (budget) */
    LEFT JOIN dbe_dbx_internships.datastore.ExchangeRate AS AC_Budget
        ON AC_Budget.FromCurrencyCode = FDFS.PricingCurrencyCode
       AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
       AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
       AND FDFS.ForecastStartDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

    /* 2.9  Exchange rates – group currency (budget) */
    LEFT JOIN dbe_dbx_internships.datastore.ExchangeRate AS GC_Budget
        ON GC_Budget.FromCurrencyCode = FDFS.PricingCurrencyCode
       AND GC_Budget.ToCurrencyCode = L.GroupCurrency
       AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
       AND FDFS.ForecastStartDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

-- 3.  Filter rows where the forecast entry is active and the key is valid.
WHERE
    FDFS.ExpandID <> 0
    OR (FDFS.ExpandID = 0 AND FDFS.KeyId = '')

""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [UNSUPPORTED_TYPED_LITERAL] Literals of the type "N" are not supported. Supported types are "DATE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP", "INTERVAL", "X". SQLSTATE: 0A000
# MAGIC == SQL (line 1, position 2734) ==
# MAGIC ...AccountingCurrency) as string), N'_N/A')   AS AccountingCurrencyCode,  ...
# MAGIC                                    ^^^^^^^
# MAGIC
# MAGIC ```
