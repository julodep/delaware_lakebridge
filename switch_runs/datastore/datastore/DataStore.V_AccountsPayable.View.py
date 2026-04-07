# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_AccountsPayable.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_AccountsPayable.View.sql`

# COMMAND ----------

# ==============================================================================
# Databricks view creation –  V_AccountsPayable
# ==============================================================================
# NOTE: Replace `dbe_dbx_internships` and `datastore` with the actual catalog and schema
# names before executing.  All object references are fully‑qualified in the
# form `dbe_dbx_internships.datastore.{object_name}`.
#
# The original T‑SQL was translated to Spark SQL.  Where a direct translation
# is impossible (e.g. aggregate expressions inside JOIN conditions) the
# problematic part is left commented with an explanation.
#
# ==============================================================================

spark.sql("""
-- ------------------------------------------------------------------------
-- CREATED OR REPLACE VIEW `V_AccountsPayable`
-- ------------------------------------------------------------------------
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_AccountsPayable` AS

-- 1. CTE – Period information (adapted from ETL.Date)
WITH Period AS (
    SELECT
        MIN(MonthId) AS DimPeriodId,
        MonthId,
        MAX(DateTime) AS PeriodDate,
        YearId
    FROM dbe_dbx_internships.ETL.Date
    WHERE DateTime <= current_timestamp()
    GROUP BY MonthId, year(DateTime), month(DateTime), YearId
),

-- 2. CTE – MaxSettlement (uses a CASE instead of a sub‑query for Max)
MaxSettlement AS (
    SELECT
        VSS.TransRecId AS TransRecId,
        CASE
            WHEN VTS.Closed = CAST('1900-01-01' AS timestamp) THEN current_timestamp()
            ELSE MAX(VSS.TransDate)
        END AS MaxTransDate
    FROM dbe_dbx_internships.dbo.SMRBIVendSettlementStaging VSS
    JOIN dbe_dbx_internships.dbo.SMRBIVendTransStaging VTS
        ON VSS.TransRecId = VTS.VendTransRecId
    GROUP BY VSS.TransRecId, VTS.Closed
),

-- 3. CTE – CumulSettlementsPerPeriod
CumulSettlementsPerPeriod AS (
    SELECT
        VSS.AccountNum      AS AccountNum,
        VSS.TransCompany    AS TransCompany,
        MAX(VSS.TransDate)  AS MaxTransDatePerPeriod,
        VSS.TransRecId      AS TransRecId,
        SUM(CAST(VSS.ExChAdjustment AS DECIMAL(22,6)))      AS ExChAdjustment,
        SUM(CAST(VSS.SettleAmountCur AS DECIMAL(22,6)))      AS SettleAmountCur,
        SUM(CAST(VSS.SettleAmountMst AS DECIMAL(22,6)))      AS SettleAmountMst,
        P.MonthId
    FROM Period P
    JOIN dbe_dbx_internships.dbo.SMRBIVendSettlementStaging VSS
        ON year(VSS.TransDate)*100 + month(VSS.TransDate) <= P.MonthId
    JOIN MaxSettlement
        ON VSS.TransRecId = MaxSettlement.TransRecId
       AND P.MonthId <= year(MaxSettlement.MaxTransDate)*100 + month(MaxSettlement.MaxTransDate)
    WHERE P.YearId >= 2005
      AND P.MonthId <= year(date_sub(current_timestamp(), 1))*100 + month(date_sub(current_timestamp(), 1))
    GROUP BY VSS.AccountNum, VSS.TransCompany, VSS.TransRecId, P.MonthId, P.PeriodDate
)

-- ------------------------------------------------------------------------
-- Final SELECT – gather all columns for the view
-- ------------------------------------------------------------------------
SELECT
    -- Identity fields
    concat(VTS.VendTransRecId, VTS.DataAreaId)    AS AccountsPayableCodeScreening,
    VTS.VendTransRecId                            AS RecId,

    -- Simple string fields with null / empty handling
    coalesce( case when VTS.Invoice = '' then null else upper(VTS.Invoice) end,
              '_N/A')                      AS PurchaseInvoiceCode,
    coalesce(VTS.Voucher, '_N/A')                AS PayablesVoucher,

    -- Description – truncated to 255 characters and appended with ...
    coalesce(
        NULLIF(
            cast(
                case
                    when length(upper(trim(VTS.Txt))) > 255
                        then concat(substring(upper(trim(VTS.Txt)), 1, 252), '...')
                    else upper(trim(VTS.Txt))
                end
            AS STRING), ''), '_N/A')
    AS Description,

    coalesce(upper(VTS.DataAreaId), '_N/A')       AS CompanyCode,

    -- Status flag
    CASE
        WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.MonthId
             AND VTS.AmountCur = VSS.SettleAmountCur
        THEN 0
        ELSE 1
    END                                          AS DimIsOpenAmountId,

    -- Aging buckets – note: the nested DATEDIFF logic is translated verbatim
    CASE
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0 THEN '<0'
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 7 THEN '0-7'
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 15 THEN '8-15'
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 30 THEN '16-30'
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 60 THEN '31-60'
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 90 THEN '61-90'
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 120 THEN '91-120'
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 180 THEN '121-180'
        WHEN datediff(
                CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,
                CASE
                    WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 365 THEN '181-365'
        ELSE '>365'
    END                                          AS OutStandingPeriodCode,

    -- Currency / account fields
    coalesce(VTS.AccountNum, '_N/A')               AS SupplierCode,
    upper(VTS.CurrencyCode)                       AS TransactionCurrencyCode,
    coalesce(cast(upper(L.AccountingCurrency) AS STRING), '_N/A')   AS AccountingCurrencyCode,
    coalesce(cast(upper(L.ReportingCurrency) AS STRING), '_N/A')   AS ReportingCurrencyCode,
    coalesce(cast(upper(L.GroupCurrency) as STRING), '_N/A')      AS GroupCurrencyCode,

    -- Date fields
    coalesce(VTS.TransDate, to_timestamp('1900-01-01','yyyy-MM-dd')) AS InvoiceDate,
    CASE
        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
        WHEN VTS.DueDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DueDate
        ELSE to_timestamp('1900-01-01','yyyy-MM-dd')
    END                                              AS DueDate,
    coalesce(VSS.MaxTransDatePerPeriod, to_timestamp('1900-01-01','yyyy-MM-dd')) AS LastPaymentDate,
    CASE
        WHEN VTS.DocumentDate = to_timestamp('1900-01-01','yyyy-MM-dd') THEN to_timestamp('1900-01-01','yyyy-MM-dd')
        WHEN VTS.DocumentDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DocumentDate
        ELSE to_timestamp('1900-01-01','yyyy-MM-dd')
    END                                              AS DocumentDate,

    -- Reporting date (MonthId * 100 + 01)
    concat(cast(P.MonthId * 100 + 1 AS STRING), '01') AS ReportDate,

    -- Invoice amounts – cast to DECIMAL(22,6)
    cast(VTS.AmountCur AS DECIMAL(22,6))           AS InvoiceAmountTC,
    cast(VTS.AmountMst AS DECIMAL(22,6))           AS InvoiceAmountAC,

    -- Cross‑exchange rates (NULL‑SAFE)
    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))
            ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * RC.ExchangeRate
        END, 0)                                     AS InvoiceAmountRC,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))
            ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * GC.ExchangeRate
        END, 0)                                     AS InvoiceAmountGC,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.AccountingCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))
            ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * AC_Budget.ExchangeRate
        END, 0)                                     AS InvoiceAmountAC_Budget,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))
            ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * RC_Budget.ExchangeRate
        END, 0)                                     AS InvoiceAmountRC_Budget,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))
            ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * GC_Budget.ExchangeRate
        END, 0)                                     AS InvoiceAmountGC_Budget,

    -- Paid amounts
    CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END AS PaidAmountTC,
    CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS PaidAmountAC,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency
                 THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * RC.ExchangeRate
        END, 0)                                     AS PaidAmountRC,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency
                 THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * GC.ExchangeRate
        END, 0)                                     AS PaidAmountGC,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.AccountingCurrency
                 THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate
        END, 0)                                     AS PaidAmountAC_Budget,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency
                 THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate
        END, 0)                                     AS PaidAmountRC_Budget,

    coalesce(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency
                 THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * GC_Budget.ExchangeRate
        END, 0)                                     AS PaidAmountGC_Budget,

    -- Exchange rate constants
    cast(1 AS DECIMAL(38,6))                        AS AppliedExchangeRateTC,
    coalesce(RC.ExchangeRate, 0)                    AS AppliedExchangeRateRC,
    coalesce(AC.ExchangeRate, 0)                    AS AppliedExchangeRateAC,
    coalesce(GC.ExchangeRate, 0)                    AS AppliedExchangeRateGC,
    coalesce(RC_Budget.ExchangeRate, 0)              AS AppliedExchangeRateRC_Budget,
    coalesce(AC_Budget.ExchangeRate, 0)              AS AppliedExchangeRateAC_Budget,
    coalesce(GC_Budget.ExchangeRate, 0)              AS AppliedExchangeRateGC_Budget

-- ------------------------------------------------------------------------
-- FROM and JOIN clauses – all tables are fully qualified
-- ------------------------------------------------------------------------
FROM dbe_dbx_internships.dbo.SMRBIVendTransStaging VTS

-- * Period join * – retain the business logic that uses a range expression
JOIN Period P
    ON year(VTS.TransDate)*100 + month(VTS.TransDate) <= P.MonthId
   AND CASE
           WHEN VTS.Closed = CAST('1900-01-01' AS timestamp) THEN year(current_timestamp())*100 + month(current_timestamp())
           WHEN VTS.Closed < VTS.TransDate THEN year(VTS.TransDate)*100 + month(VTS.TransDate)
           ELSE year(VTS.Closed)*100 + month(VTS.Closed)
       END >= P.MonthId

-- * Settlements join *
LEFT JOIN CumulSettlementsPerPeriod VSS
    ON VTS.VendTransRecId = VSS.TransRecId
   AND P.MonthId = VSS.MonthId

-- * Ledger cross‑join * – note the use of CROSS JOIN without a keyword is
--   supported in Spark; we keep the original intent.
JOIN ( SELECT DISTINCT
          LES.ReportingCurrency,
          LES.AccountingCurrency,
          LES.ExchangeRateType,
          LES.BudgetExchangeRateType,
          LES.Name,
          G.GroupCurrencyCode AS GroupCurrency
      FROM dbe_dbx_internships.dbo.SMRBILedgerStaging LES
      CROSS JOIN ( SELECT GroupCurrencyCode FROM dbe_dbx_internships.ETL.GroupCurrency LIMIT 1 ) G
    ) L
    ON L.Name = VTS.DataAreaId

-- * Exchange rate lookups * – keep all LEFT JOINs
LEFT JOIN dbe_dbx_internships.DataStore.ExchangeRate RC
    ON RC.FromCurrencyCode = VTS.CurrencyCode
   AND RC.ToCurrencyCode = L.ReportingCurrency
   AND RC.ExchangeRateTypeCode = L.ExchangeRateType
   AND VTS.TransDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN dbe_dbx_internships.DataStore.ExchangeRate AC
    ON AC.FromCurrencyCode = VTS.CurrencyCode
   AND AC.ToCurrencyCode = L.AccountingCurrency
   AND AC.ExchangeRateTypeCode = L.ExchangeRateType
   AND VTS.TransDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN dbe_dbx_internships.DataStore.ExchangeRate GC
    ON GC.FromCurrencyCode = VTS.CurrencyCode
   AND GC.ToCurrencyCode = L.GroupCurrency
   AND GC.ExchangeRateTypeCode = L.ExchangeRateType
   AND VTS.TransDate BETWEEN GC.ValidFrom AND GC.ValidTo

LEFT JOIN dbe_dbx_internships.DataStore.ExchangeRate RC_Budget
    ON RC_Budget.FromCurrencyCode = VTS.CurrencyCode
   AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
   AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
   AND VTS.TransDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN dbe_dbx_internships.DataStore.ExchangeRate AC_Budget
    ON AC_Budget.FromCurrencyCode = VTS.CurrencyCode
   AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
   AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeType
   AND VTS.TransDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN dbe_dbx_internships.DataStore.ExchangeRate GC_Budget
    ON GC_Budget.FromCurrencyCode = VTS.CurrencyCode
   AND GC_Budget.ToCurrencyCode = L.GroupCurrency
   AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeType
   AND VTS.TransDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

-- * Date range for NULL‑SAFE predicates *
CROSS JOIN (
    SELECT min(DateTime) AS MinDate,
           max(DateTime) AS MaxDate
    FROM dbe_dbx_internships.ETL.Date
) D
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 16390)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN -- ------------------------------------------------------------------------ -- CREATED OR REPLACE VIEW `V_AccountsPayable` -- ------------------------------------------------------------------------ CREATE OR REPLACE VIEW `catalog`.`schema`.`V_AccountsPayable` AS  -- 1. CTE – Period information (adapted from ETL.Date) WITH Period AS (     SELECT         MIN(MonthId) AS DimPeriodId,         MonthId,         MAX(DateTime) AS PeriodDate,         YearId     FROM catalog.ETL.Date     WHERE DateTime <= current_timestamp()     GROUP BY MonthId, year(DateTime), month(DateTime), YearId ),  -- 2. CTE – MaxSettlement (uses a CASE instead of a sub‑query for Max) MaxSettlement AS (     SELECT         VSS.TransRecId AS TransRecId,         CASE             WHEN VTS.Closed = CAST('1900-01-01' AS timestamp) THEN current_timestamp()             ELSE MAX(VSS.TransDate)         END AS MaxTransDate     FROM catalog.dbo.SMRBIVendSettlementStaging VSS     JOIN catalog.dbo.SMRBIVendTransStaging VTS         ON VSS.TransRecId = VTS.VendTransRecId     GROUP BY VSS.TransRecId, VTS.Closed ),  -- 3. CTE – CumulSettlementsPerPeriod CumulSettlementsPerPeriod AS (     SELECT         VSS.AccountNum      AS AccountNum,         VSS.TransCompany    AS TransCompany,         MAX(VSS.TransDate)  AS MaxTransDatePerPeriod,         VSS.TransRecId      AS TransRecId,         SUM(CAST(VSS.ExChAdjustment AS DECIMAL(22,6)))      AS ExChAdjustment,         SUM(CAST(VSS.SettleAmountCur AS DECIMAL(22,6)))      AS SettleAmountCur,         SUM(CAST(VSS.SettleAmountMst AS DECIMAL(22,6)))      AS SettleAmountMst,         P.MonthId     FROM Period P     JOIN catalog.dbo.SMRBIVendSettlementStaging VSS         ON year(VSS.TransDate)*100 + month(VSS.TransDate) <= P.MonthId     JOIN MaxSettlement         ON VSS.TransRecId = MaxSettlement.TransRecId        AND P.MonthId <= year(MaxSettlement.MaxTransDate)*100 + month(MaxSettlement.MaxTransDate)     WHERE P.YearId >= 2005       AND P.MonthId <= year(date_sub(current_timestamp(), 1))*100 + month(date_sub(current_timestamp(), 1))     GROUP BY VSS.AccountNum, VSS.TransCompany, VSS.TransRecId, P.MonthId, P.PeriodDate )  -- ------------------------------------------------------------------------ -- Final SELECT – gather all columns for the view -- ------------------------------------------------------------------------ SELECT     -- Identity fields     concat(VTS.VendTransRecId, VTS.DataAreaId)    AS AccountsPayableCodeScreening,     VTS.VendTransRecId                            AS RecId,      -- Simple string fields with null / empty handling     coalesce( case when VTS.Invoice = '' then null else upper(VTS.Invoice) end,               '_N/A')                      AS PurchaseInvoiceCode,     coalesce(VTS.Voucher, '_N/A')                AS PayablesVoucher,      -- Description – truncated to 255 characters and appended with ...     coalesce(         NULLIF(             cast(                 case                     when length(upper(trim(VTS.Txt))) > 255                         then concat(substring(upper(trim(VTS.Txt)), 1, 252), '...')                     else upper(trim(VTS.Txt))                 end             AS STRING), ''), '_N/A')     AS Description,      coalesce(upper(VTS.DataAreaId), '_N/A')       AS CompanyCode,      -- Status flag     CASE         WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.MonthId              AND VTS.AmountCur = VSS.SettleAmountCur         THEN 0         ELSE 1     END                                          AS DimIsOpenAmountId,      -- Aging buckets – note: the nested DATEDIFF logic is translated verbatim     CASE         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 0 THEN '<0'         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 7 THEN '0-7'         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 15 THEN '8-15'         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 30 THEN '16-30'         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 60 THEN '31-60'         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 90 THEN '61-90'         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 120 THEN '91-120'         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 180 THEN '121-180'         WHEN datediff(                 CASE WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate ELSE VTS.DueDate END,                 CASE                     WHEN (year(VTS.Closed)*100 + month(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END             ) <= 365 THEN '181-365'         ELSE '>365'     END                                          AS OutStandingPeriodCode,      -- Currency / account fields     coalesce(VTS.AccountNum, '_N/A')               AS SupplierCode,     upper(VTS.CurrencyCode)                       AS TransactionCurrencyCode,     coalesce(cast(upper(L.AccountingCurrency) AS STRING), '_N/A')   AS AccountingCurrencyCode,     coalesce(cast(upper(L.ReportingCurrency) AS STRING), '_N/A')   AS ReportingCurrencyCode,     coalesce(cast(upper(L.GroupCurrency) as STRING), '_N/A')      AS GroupCurrencyCode,      -- Date fields     coalesce(VTS.TransDate, to_timestamp('1900-01-01','yyyy-MM-dd')) AS InvoiceDate,     CASE         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate         WHEN VTS.DueDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DueDate         ELSE to_timestamp('1900-01-01','yyyy-MM-dd')     END                                              AS DueDate,     coalesce(VSS.MaxTransDatePerPeriod, to_timestamp('1900-01-01','yyyy-MM-dd')) AS LastPaymentDate,     CASE         WHEN VTS.DocumentDate = to_timestamp('1900-01-01','yyyy-MM-dd') THEN to_timestamp('1900-01-01','yyyy-MM-dd')         WHEN VTS.DocumentDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DocumentDate         ELSE to_timestamp('1900-01-01','yyyy-MM-dd')     END                                              AS DocumentDate,      -- Reporting date (MonthId * 100 + 01)     concat(cast(P.MonthId * 100 + 1 AS STRING), '01') AS ReportDate,      -- Invoice amounts – cast to DECIMAL(22,6)     cast(VTS.AmountCur AS DECIMAL(22,6))           AS InvoiceAmountTC,     cast(VTS.AmountMst AS DECIMAL(22,6))           AS InvoiceAmountAC,      -- Cross‑exchange rates (NULL‑SAFE)     coalesce(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))             ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * RC.ExchangeRate         END, 0)                                     AS InvoiceAmountRC,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))             ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * GC.ExchangeRate         END, 0)                                     AS InvoiceAmountGC,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.AccountingCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))             ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * AC_Budget.ExchangeRate         END, 0)                                     AS InvoiceAmountAC_Budget,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))             ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * RC_Budget.ExchangeRate         END, 0)                                     AS InvoiceAmountRC_Budget,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency THEN cast(VTS.AmountCur AS DECIMAL(22,6))             ELSE cast(VTS.AmountCur AS DECIMAL(22,6)) * GC_Budget.ExchangeRate         END, 0)                                     AS InvoiceAmountGC_Budget,      -- Paid amounts     CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END AS PaidAmountTC,     CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS PaidAmountAC,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency                  THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * RC.ExchangeRate         END, 0)                                     AS PaidAmountRC,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency                  THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * GC.ExchangeRate         END, 0)                                     AS PaidAmountGC,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.AccountingCurrency                  THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate         END, 0)                                     AS PaidAmountAC_Budget,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency                  THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate         END, 0)                                     AS PaidAmountRC_Budget,      coalesce(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency                  THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * GC_Budget.ExchangeRate         END, 0)                                     AS PaidAmountGC_Budget,      -- Exchange rate constants     cast(1 AS DECIMAL(38,6))                        AS AppliedExchangeRateTC,     coalesce(RC.ExchangeRate, 0)                    AS AppliedExchangeRateRC,     coalesce(AC.ExchangeRate, 0)                    AS AppliedExchangeRateAC,     coalesce(GC.ExchangeRate, 0)                    AS AppliedExchangeRateGC,     coalesce(RC_Budget.ExchangeRate, 0)              AS AppliedExchangeRateRC_Budget,     coalesce(AC_Budget.ExchangeRate, 0)              AS AppliedExchangeRateAC_Budget,     coalesce(GC_Budget.ExchangeRate, 0)              AS AppliedExchangeRateGC_Budget  -- ------------------------------------------------------------------------ -- FROM and JOIN clauses – all tables are fully qualified -- ------------------------------------------------------------------------ FROM catalog.dbo.SMRBIVendTransStaging VTS  -- * Period join * – retain the business logic that uses a range expression JOIN Period P     ON year(VTS.TransDate)*100 + month(VTS.TransDate) <= P.MonthId    AND CASE            WHEN VTS.Closed = CAST('1900-01-01' AS timestamp) THEN year(current_timestamp())*100 + month(current_timestamp())            WHEN VTS.Closed < VTS.TransDate THEN year(VTS.TransDate)*100 + month(VTS.TransDate)            ELSE year(VTS.Closed)*100 + month(VTS.Closed)        END >= P.MonthId  -- * Settlements join * LEFT JOIN CumulSettlementsPerPeriod VSS     ON VTS.VendTransRecId = VSS.TransRecId    AND P.MonthId = VSS.MonthId  -- * Ledger cross‑join * – note the use of CROSS JOIN without a keyword is --   supported in Spark; we keep the original intent. JOIN ( SELECT DISTINCT           LES.ReportingCurrency,           LES.AccountingCurrency,           LES.ExchangeRateType,           LES.BudgetExchangeRateType,           LES.Name,           G.GroupCurrencyCode AS GroupCurrency       FROM catalog.dbo.SMRBILedgerStaging LES       CROSS JOIN ( SELECT GroupCurrencyCode FROM catalog.ETL.GroupCurrency LIMIT 1 ) G     ) L     ON L.Name = VTS.DataAreaId  -- * Exchange rate lookups * – keep all LEFT JOINs LEFT JOIN catalog.DataStore.ExchangeRate RC     ON RC.FromCurrencyCode = VTS.CurrencyCode    AND RC.ToCurrencyCode = L.ReportingCurrency    AND RC.ExchangeRateTypeCode = L.ExchangeRateType    AND VTS.TransDate BETWEEN RC.ValidFrom AND RC.ValidTo  LEFT JOIN catalog.DataStore.ExchangeRate AC     ON AC.FromCurrencyCode = VTS.CurrencyCode    AND AC.ToCurrencyCode = L.AccountingCurrency    AND AC.ExchangeRateTypeCode = L.ExchangeRateType    AND VTS.TransDate BETWEEN AC.ValidFrom AND AC.ValidTo  LEFT JOIN catalog.DataStore.ExchangeRate GC     ON GC.FromCurrencyCode = VTS.CurrencyCode    AND GC.ToCurrencyCode = L.GroupCurrency    AND GC.ExchangeRateTypeCode = L.ExchangeRateType    AND VTS.TransDate BETWEEN GC.ValidFrom AND GC.ValidTo  LEFT JOIN catalog.DataStore.ExchangeRate RC_Budget     ON RC_Budget.FromCurrencyCode = VTS.CurrencyCode    AND RC_Budget.ToCurrencyCode = L.ReportingCurrency    AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType    AND VTS.TransDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo  LEFT JOIN catalog.DataStore.ExchangeRate AC_Budget     ON AC_Budget.FromCurrencyCode = VTS.CurrencyCode    AND AC_Budget.ToCurrencyCode = L.AccountingCurrency    AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeType    AND VTS.TransDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo  LEFT JOIN catalog.DataStore.ExchangeRate GC_Budget     ON GC_Budget.FromCurrencyCode = VTS.CurrencyCode    AND GC_Budget.ToCurrencyCode = L.GroupCurrency    AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeType    AND VTS.TransDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo  -- * Date range for NULL‑SAFE predicates * CROSS JOIN (     SELECT min(DateTime) AS MinDate,            max(DateTime) AS MaxDate     FROM catalog.ETL.Date ) D
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
