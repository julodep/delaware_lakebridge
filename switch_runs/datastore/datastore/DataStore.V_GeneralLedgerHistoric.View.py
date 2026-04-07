# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_GeneralLedgerHistoric.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_GeneralLedgerHistoric.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks compatible view creation – V_GeneralLedgerHistoric
# ------------------------------------------------------------------
# All object names are fully-qualified: dbe_dbx_internships.datastore.{object}.
# The view definition has been translated to Spark-SQL syntax.
# Functions that have a direct equivalent in Spark are used:
#   * NVL (instead of ISNULL)
#   * UPPER, CAST, COALESCE, CASE
# The view is created with CREATE OR REPLACE VIEW, so it can be
# executed repeatedly without error.
# ------------------------------------------------------------------

view_sql = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_GeneralLedgerHistoric` AS
SELECT
    -- 1. RecId and constant fields
    CAST(-1 AS INT) AS RecId,
    CAST('_N/A' AS STRING) AS TransactionCode,

    -- 2. Company information (NVL + UPPER where requested)
    NVL(UPPER(CAST(CompanyCode AS STRING)), '_N/A') AS CompanyCode,
    UPPER(CAST(L.ExchangeRateType AS STRING))          AS DefaultExchangeRateTypeCode,
    UPPER(CAST(L.BudgetExchangeRateType AS STRING))    AS BudgetExchangeRateTypeCode,
    UPPER(CAST(LES.AccountingCurrency AS STRING))     AS TransactionCurrencyCode,
    NVL(UPPER(CAST(GL.AccountingCurrencyCode AS STRING)), '_N/A') AS AccountingCurrencyCode,
    NVL(UPPER(CAST(LES.ReportingCurrency AS STRING)), '_N/A')     AS ReportingCurrencyCode,
    NVL(UPPER(CAST(L.GroupCurrency AS STRING)), '_N/A')              AS GroupCurrencyCode,
    NVL(UPPER(CAST(GLAccountCode AS STRING)), '_N/A')                AS GLAccountCode,
    NVL(UPPER(CAST(IntercompanyCode AS STRING)), '_N/A')              AS InterCompanyCode,
    NVL(UPPER(CAST(BusinessSegmentCode AS STRING)), '_N/A')          AS BusinessSegmentCode,
    NVL(UPPER(CAST(DepartmentCode AS STRING)), '_N/A')                AS DepartmentCode,
    NVL(UPPER(CAST(EndCustomerCode AS STRING)), '_N/A')              AS EndCustomerCode,
    NVL(UPPER(CAST(LocationCode AS STRING)), '_N/A')                 AS LocationCode,
    NVL(UPPER(CAST(ShipmentContractCode AS STRING)), '_N/A')         AS ShipmentContractCode,
    NVL(UPPER(CAST(LocalAccountCode AS STRING)), '_N/A')             AS LocalAccountCode,
    NVL(UPPER(CAST(ProductCode AS STRING)), '_N/A')                  AS ProductFDCode,

    -- 3. Date & identifiers
    CAST('1900-01-01' AS TIMESTAMP) AS DocumentDate,
    CAST(DimPostingDateId AS INT)   AS DimPostingDateId,

    -- 4. Constant voucher
    '_N/A' AS Voucher,

    -- 5. Amount calculations (use COALESCE & CASE for NULL handling)
    COALESCE(
        CASE
            WHEN GL.AccountingCurrencyCode = LES.AccountingCurrency
                THEN CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6))
            ELSE CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6)) * TC.ExchangeRate
        END,
        0
    ) AS AmountTC,

    CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6)) AS AmountAC,
    CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6)) AS AmountRC,

    COALESCE(
        CASE
            WHEN GL.AccountingCurrencyCode = L.GroupCurrency
                THEN CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6))
            ELSE CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6)) * GC.ExchangeRate
        END,
        0
    ) AS AmountGC,

    COALESCE(TC.ExchangeRate, 1) AS AppliedExchangeRateTC,
    CAST(1 AS DECIMAL(38,17))      AS AppliedExchangeRateAC,
    CAST(1 AS DECIMAL(38,17))      AS AppliedExchangeRateRC,
    COALESCE(GC.ExchangeRate, 1)   AS AppliedExchangeRateGC

FROM `dbe_dbx_internships`.`datastore`.GeneralLedger GL

-- 6. Left join to the staging tables (SMRBILedgerStaging)
LEFT JOIN (
    SELECT DISTINCT *
    FROM `dbe_dbx_internships`.`datastore`.SMRBILedgerStaging
) LES
    ON GL.CompanyCode = LES.Name

-- 7. Join to the lookup table that enriches company with currency data
LEFT JOIN (
    SELECT DISTINCT
        LES.ReportingCurrency,
        LES.AccountingCurrency,
        LES.ExchangeRateType,
        LES.BudgetExchangeRateType,
        LES.Name,
        G.GroupCurrencyCode AS GroupCurrency
    FROM `dbe_dbx_internships`.`datastore`.SMRBILedgerStaging LES
    CROSS JOIN (
        SELECT * FROM `dbe_dbx_internships`.`datastore`.GroupCurrency LIMIT 1
    ) G
) L
    ON L.Name = GL.CompanyCode

-- 8. Exchange rate look-ups (LOOSE join using BETWEEN)
LEFT JOIN `dbe_dbx_internships`.`datastore`.ExchangeRate GC
    ON GC.FromCurrencyCode = GL.AccountingCurrencyCode
   AND GC.ToCurrencyCode = L.GroupCurrency
   AND GC.ExchangeRateTypeCode = L.ExchangeRateType
   AND GL.DimPostingDateId BETWEEN GC.ValidFrom AND GC.ValidTo

LEFT JOIN `dbe_dbx_internships`.`datastore`.ExchangeRate TC
    ON TC.FromCurrencyCode = GL.AccountingCurrencyCode
   AND TC.ToCurrencyCode = LES.AccountingCurrency
   AND TC.ExchangeRateTypeCode = L.ExchangeRateType
   AND GL.DimPostingDateId BETWEEN TC.ValidFrom AND TC.ValidTo;
"""

# COMMAND ----------

# Execute the view definition on the target catalog and schema
spark.sql(view_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 4255)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_GeneralLedgerHistoric` AS SELECT     -- 1. RecId and constant fields     CAST(-1 AS INT) AS RecId,     CAST('_N/A' AS STRING) AS TransactionCode,      -- 2. Company information (NVL + UPPER where requested)     NVL(UPPER(CAST(CompanyCode AS STRING)), '_N/A') AS CompanyCode,     UPPER(CAST(L.ExchangeRateType AS STRING))          AS DefaultExchangeRateTypeCode,     UPPER(CAST(L.BudgetExchangeRateType AS STRING))    AS BudgetExchangeRateTypeCode,     UPPER(CAST(LES.AccountingCurrency AS STRING))     AS TransactionCurrencyCode,     NVL(UPPER(CAST(GL.AccountingCurrencyCode AS STRING)), '_N/A') AS AccountingCurrencyCode,     NVL(UPPER(CAST(LES.ReportingCurrency AS STRING)), '_N/A')     AS ReportingCurrencyCode,     NVL(UPPER(CAST(L.GroupCurrency AS STRING)), '_N/A')              AS GroupCurrencyCode,     NVL(UPPER(CAST(GLAccountCode AS STRING)), '_N/A')                AS GLAccountCode,     NVL(UPPER(CAST(IntercompanyCode AS STRING)), '_N/A')              AS InterCompanyCode,     NVL(UPPER(CAST(BusinessSegmentCode AS STRING)), '_N/A')          AS BusinessSegmentCode,     NVL(UPPER(CAST(DepartmentCode AS STRING)), '_N/A')                AS DepartmentCode,     NVL(UPPER(CAST(EndCustomerCode AS STRING)), '_N/A')              AS EndCustomerCode,     NVL(UPPER(CAST(LocationCode AS STRING)), '_N/A')                 AS LocationCode,     NVL(UPPER(CAST(ShipmentContractCode AS STRING)), '_N/A')         AS ShipmentContractCode,     NVL(UPPER(CAST(LocalAccountCode AS STRING)), '_N/A')             AS LocalAccountCode,     NVL(UPPER(CAST(ProductCode AS STRING)), '_N/A')                  AS ProductFDCode,      -- 3. Date & identifiers     CAST('1900-01-01' AS TIMESTAMP) AS DocumentDate,     CAST(DimPostingDateId AS INT)   AS DimPostingDateId,      -- 4. Constant voucher     '_N/A' AS Voucher,      -- 5. Amount calculations (use COALESCE & CASE for NULL handling)     COALESCE(         CASE             WHEN GL.AccountingCurrencyCode = LES.AccountingCurrency                 THEN CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6))             ELSE CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6)) * TC.ExchangeRate         END,         0     ) AS AmountTC,      CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6)) AS AmountAC,     CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6)) AS AmountRC,      COALESCE(         CASE             WHEN GL.AccountingCurrencyCode = L.GroupCurrency                 THEN CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6))             ELSE CAST(REPLACE(amountAC, ',', '.') AS DECIMAL(38,6)) * GC.ExchangeRate         END,         0     ) AS AmountGC,      COALESCE(TC.ExchangeRate, 1) AS AppliedExchangeRateTC,     CAST(1 AS DECIMAL(38,17))      AS AppliedExchangeRateAC,     CAST(1 AS DECIMAL(38,17))      AS AppliedExchangeRateRC,     COALESCE(GC.ExchangeRate, 1)   AS AppliedExchangeRateGC  FROM `_placeholder_`.`_placeholder_`.GeneralLedger GL  -- 6. Left join to the staging tables (SMRBILedgerStaging) LEFT JOIN (     SELECT DISTINCT *     FROM `_placeholder_`.`_placeholder_`.SMRBILedgerStaging ) LES     ON GL.CompanyCode = LES.Name  -- 7. Join to the lookup table that enriches company with currency data LEFT JOIN (     SELECT DISTINCT         LES.ReportingCurrency,         LES.AccountingCurrency,         LES.ExchangeRateType,         LES.BudgetExchangeRateType,         LES.Name,         G.GroupCurrencyCode AS GroupCurrency     FROM `_placeholder_`.`_placeholder_`.SMRBILedgerStaging LES     CROSS JOIN (         SELECT * FROM `_placeholder_`.`_placeholder_`.GroupCurrency LIMIT 1     ) G ) L     ON L.Name = GL.CompanyCode  -- 8. Exchange rate look-ups (LOOSE join using BETWEEN) LEFT JOIN `_placeholder_`.`_placeholder_`.ExchangeRate GC     ON GC.FromCurrencyCode = GL.AccountingCurrencyCode    AND GC.ToCurrencyCode = L.GroupCurrency    AND GC.ExchangeRateTypeCode = L.ExchangeRateType    AND GL.DimPostingDateId BETWEEN GC.ValidFrom AND GC.ValidTo  LEFT JOIN `_placeholder_`.`_placeholder_`.ExchangeRate TC     ON TC.FromCurrencyCode = GL.AccountingCurrencyCode    AND TC.ToCurrencyCode = LES.AccountingCurrency    AND TC.ExchangeRateTypeCode = L.ExchangeRateType    AND GL.DimPostingDateId BETWEEN TC.ValidFrom AND TC.ValidTo;
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
