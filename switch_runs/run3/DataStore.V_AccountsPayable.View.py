# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_AccountsPayable.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323134003-blyy/DataStore.V_AccountsPayable.View.sql`

# COMMAND ----------

# ----------------------------------------------
# Setup: import anything needed (Databricks provides `spark` by default)
# ----------------------------------------------

# -------------------------------------------------------------------------
# Create or replace the view `DataStore.V_AccountsPayable`
# Adjusted for Spark SQL compatibility.
# -------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE VIEW DataStore.V_AccountsPayable AS
WITH Period AS (
    SELECT
        MIN(MonthId)               AS DimPeriodId,
        MonthId,
        MAX(DateTime)              AS PeriodDate,
        YearId
    FROM ETL.Date
    WHERE DateTime <= current_timestamp()
    GROUP BY MonthId, YEAR(DateTime), MONTH(DateTime), YearId
),

MaxSettlement AS (
    SELECT
        VSS.TransRecId AS TransRecId,
        CASE
            WHEN VTS.Closed = DATE('1900-01-01') THEN current_timestamp()
            ELSE MAX(VSS.TransDate)
        END AS MaxTransDate
    FROM dbo.SMRBIVendSettlementStaging VSS
    JOIN dbo.SMRBIVendTransStaging VTS
        ON VSS.TransRecId = VTS.VendTransRecId
    GROUP BY VSS.TransRecId, VTS.Closed
),

CumulSettlementsPerPeriod AS (
    SELECT
        VSS.AccountNum                                   AS AccountNum,
        VSS.TransCompany                                 AS TransCompany,
        MAX(VSS.TransDate)                               AS MaxTransDatePerPeriod,
        VSS.TransRecId                                   AS TransRecId,
        SUM(CAST(VSS.ExChAdjustment AS DECIMAL(22,6)))   AS ExChAdjustment,
        SUM(CAST(VSS.SettleAmountCur AS DECIMAL(22,6)))  AS SettleAmountCur,
        SUM(CAST(VSS.SettleAmountMst AS DECIMAL(22,6)))  AS SettleAmountMst,
        P.MonthId
    FROM Period P
    JOIN dbo.SMRBIVendSettlementStaging VSS
        ON YEAR(VSS.TransDate) * 100 + MONTH(VSS.TransDate) <= P.MonthId
    JOIN MaxSettlement
        ON VSS.TransRecId = MaxSettlement.TransRecId
        AND P.MonthId <= YEAR(MaxSettlement.MaxTransDate) * 100 + MONTH(MaxSettlement.MaxTransDate)
    WHERE
        P.YearId >= 2005
        AND P.MonthId <= YEAR(date_sub(current_date(),1)) * 100 + MONTH(date_sub(current_date(),1))
    GROUP BY
        VSS.AccountNum,
        VSS.TransCompany,
        VSS.TransRecId,
        P.MonthId,
        P.PeriodDate
)

SELECT
    concat(VTS.VendTransRecId, VTS.DataAreaId)                      AS AccountsPayableCodeScreening,
    VTS.VendTransRecId                                              AS RecId,
    NVL(
        CASE WHEN VTS.Invoice = '' THEN NULL ELSE upper(VTS.Invoice) END,
        '_N/A'
    )                                                               AS PurchaseInvoiceCode,
    NVL(VTS.Voucher, '_N/A')                                        AS PayablesVoucher,
    NVL(
        CASE
            WHEN LENGTH(TRIM(upper(VTS.Txt))) > 255
            THEN CONCAT(SUBSTRING(TRIM(upper(VTS.Txt)), 1, 252), '...')
            ELSE TRIM(upper(VTS.Txt))
        END,
        '_N/A'
    )                                                               AS Description,
    NVL(upper(VTS.DataAreaId), '_N/A')                              AS CompanyCode,
    CASE
        WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.MonthId
             AND VTS.AmountCur = VSS.SettleAmountCur THEN 0
        ELSE 1
    END                                                            AS DimIsOpenAmountId,

    -- Outstanding period calculation (converted DATEDIFF logic)
    CASE
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                CASE
                    WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                    ELSE VTS.DueDate
                END
            ) <= 0 THEN '<0'
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END,
                    7
                )
            ) <= 0 THEN '0-7'
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END,
                    15
                )
            ) <= 0 THEN '8-15'
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END,
                    30
                )
            ) <= 0 THEN '16-30'
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END,
                    60
                )
            ) <= 0 THEN '31-60'
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END,
                    90
                )
            ) <= 0 THEN '61-90'
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END,
                    120
                )
            ) <= 0 THEN '91-120'
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END,
                    180
                )
            ) <= 0 THEN '121-180'
        WHEN datediff(
                CASE
                    WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END,
                    365
                )
            ) <= 0 THEN '181-365'
        ELSE '>365'
    END                                                            AS OutStandingPeriodCode,

    NVL(VTS.AccountNum, '_N/A')                                    AS SupplierCode,
    upper(VTS.CurrencyCode)                                        AS TransactionCurrencyCode,
    NVL(upper(L.AccountingCurrency), '_N/A')                       AS AccountingCurrencyCode,
    NVL(upper(L.ReportingCurrency), '_N/A')                        AS ReportingCurrencyCode,
    NVL(upper(L.GroupCurrency), '_N/A')                            AS GroupCurrencyCode,
    NVL(VTS.TransDate, DATE('1900-01-01'))                         AS InvoiceDate,
    CASE
        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
        WHEN VTS.DueDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DueDate
        ELSE DATE('1900-01-01')
    END                                                            AS DueDate,
    NVL(VSS.MaxTransDatePerPeriod, DATE('1900-01-01'))             AS LastPaymentDate,
    CASE
        WHEN VTS.DocumentDate = DATE('1900-01-01') THEN DATE('1900-01-01')
        WHEN VTS.DocumentDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DocumentDate
        ELSE DATE('1900-01-01')
    END                                                            AS DocumentDate,
    P.DimPeriodId * 100 + 1                                        AS ReportDate,
    CAST(VTS.AmountCur AS DECIMAL(22,6))                           AS InvoiceAmountTC,
    CAST(VTS.AmountMst AS DECIMAL(22,6))                           AS InvoiceAmountAC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * RC.ExchangeRate
        END,
        0
    )                                                               AS InvoiceAmountRC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * GC.ExchangeRate
        END,
        0
    )                                                               AS InvoiceAmountGC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.AccountingCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * AC_Budget.ExchangeRate
        END,
        0
    )                                                               AS InvoiceAmountAC_Budget,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * RC_Budget.ExchangeRate
        END,
        0
    )                                                               AS InvoiceAmountRC_Budget,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * GC_Budget.ExchangeRate
        END,
        0
    )                                                               AS InvoiceAmountGC_Budget,

    CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END AS PaidAmountTC,
    CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS PaidAmountAC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency THEN
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * RC.ExchangeRate
        END,
        0
    )                                                               AS PaidAmountRC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency THEN
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * GC.ExchangeRate
        END,
        0
    )                                                               AS PaidAmountGC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.AccountingCurrency THEN
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate
        END,
        0
    )                                                               AS PaidAmountAC_Budget,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency THEN
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate
        END,
        0
    )                                                               AS PaidAmountRC_Budget,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency THEN
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate
        END,
        0
    )                                                               AS PaidAmountGC_Budget,

    CAST(VTS.AmountCur AS DECIMAL(22,6)) -
        CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END AS OpenAmountTC,

    CAST(VTS.AmountMst AS DECIMAL(22,6)) +
        CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.ExChAdjustment END -
        CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS OpenAmountAC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency THEN
                CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                RC.ExchangeRate
        END,
        0
    )                                                               AS OpenAmountRC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency THEN
                CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                GC.ExchangeRate
        END,
        0
    )                                                               AS OpenAmountGC,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.AccountingCurrency THEN
                CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                AC_Budget.ExchangeRate
        END,
        0
    )                                                               AS OpenAmountAC_Budget,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency THEN
                CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                RC_Budget.ExchangeRate
        END,
        0
    )                                                               AS OpenAmountRC_Budget,

    NVL(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency THEN
                CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE
                (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                GC_Budget.ExchangeRate
        END,
        0
    )                                                               AS OpenAmountGC_Budget,

    CAST(1 AS DECIMAL(38,6))                                        AS AppliedExchangeRateTC,
    NVL(RC.ExchangeRate, 0)                                         AS AppliedExchangeRateRC,
    NVL(AC.ExchangeRate, 0)                                         AS AppliedExchangeRateAC,
    NVL(GC.ExchangeRate, 0)                                         AS AppliedExchangeRateGC,
    NVL(RC_Budget.ExchangeRate, 0)                                  AS AppliedExchangeRateRC_Budget,
    NVL(AC_Budget.ExchangeRate, 0)                                  AS AppliedExchangeRateAC_Budget,
    NVL(GC_Budget.ExchangeRate, 0)                                  AS AppliedExchangeRateGC_Budget

FROM dbo.SMRBIVendTransStaging VTS
JOIN Period P
    ON YEAR(VTS.TransDate) * 100 + MONTH(VTS.TransDate) <= P.MonthId
    AND CASE
            WHEN VTS.Closed = DATE('1900-01-01') THEN 999999
            WHEN VTS.Closed < VTS.TransDate THEN YEAR(VTS.TransDate) * 100 + MONTH(VTS.TransDate)
            ELSE YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)
        END >= P.MonthId
LEFT JOIN CumulSettlementsPerPeriod VSS
    ON VTS.VendTransRecId = VSS.TransRecId
    AND P.MonthId = VSS.MonthId

-- Lookup tables for currency exchange rates
JOIN (
    SELECT DISTINCT
        LES.ReportingCurrency,
        LES.AccountingCurrency,
        LES.ExchangeRateType,
        LES.BudgetExchangeRateType,
        LES.Name,
        G.GroupCurrencyCode AS GroupCurrency
    FROM dbo.SMRBILedgerStaging LES
    CROSS JOIN (SELECT GroupCurrencyCode FROM ETL.GroupCurrency LIMIT 1) G
) L
    ON L.Name = VTS.DataAreaId

LEFT JOIN DataStore.ExchangeRate RC
    ON RC.FromCurrencyCode = VTS.CurrencyCode
    AND RC.ToCurrencyCode = L.ReportingCurrency
    AND RC.ExchangeRateTypeCode = L.ExchangeRateType
    AND VTS.TransDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC
    ON AC.FromCurrencyCode = VTS.CurrencyCode
    AND AC.ToCurrencyCode = L.AccountingCurrency
    AND AC.ExchangeRateTypeCode = L.ExchangeRateType
    AND VTS.TransDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC
    ON GC.FromCurrencyCode = VTS.CurrencyCode
    AND GC.ToCurrencyCode = L.GroupCurrency
    AND GC.ExchangeRateTypeCode = L.ExchangeRateType
    AND VTS.TransDate BETWEEN GC.ValidFrom AND GC.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_Budget
    ON RC_Budget.FromCurrencyCode = VTS.CurrencyCode
    AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
    AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
    AND VTS.TransDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget
    ON AC_Budget.FromCurrencyCode = VTS.CurrencyCode
    AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
    AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
    AND VTS.TransDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget
    ON GC_Budget.FromCurrencyCode = VTS.CurrencyCode
    AND GC_Budget.ToCurrencyCode = L.GroupCurrency
    AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
    AND VTS.TransDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

CROSS JOIN (
    SELECT
        MIN(DateTime) AS MinDate,
        MAX(DateTime) AS MaxDate
    FROM ETL.Date
    WHERE YearId > 1900 AND YearId < 9999
) D

WHERE
    P.YearId >= 2005
    AND P.MonthId <= YEAR(date_sub(current_date(),1)) * 100 + MONTH(date_sub(current_date(),1))
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 19960)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW DataStore.V_AccountsPayable AS WITH Period AS (     SELECT         MIN(MonthId)               AS DimPeriodId,         MonthId,         MAX(DateTime)              AS PeriodDate,         YearId     FROM ETL.Date     WHERE DateTime <= current_timestamp()     GROUP BY MonthId, YEAR(DateTime), MONTH(DateTime), YearId ),  MaxSettlement AS (     SELECT         VSS.TransRecId AS TransRecId,         CASE             WHEN VTS.Closed = DATE('1900-01-01') THEN current_timestamp()             ELSE MAX(VSS.TransDate)         END AS MaxTransDate     FROM dbo.SMRBIVendSettlementStaging VSS     JOIN dbo.SMRBIVendTransStaging VTS         ON VSS.TransRecId = VTS.VendTransRecId     GROUP BY VSS.TransRecId, VTS.Closed ),  CumulSettlementsPerPeriod AS (     SELECT         VSS.AccountNum                                   AS AccountNum,         VSS.TransCompany                                 AS TransCompany,         MAX(VSS.TransDate)                               AS MaxTransDatePerPeriod,         VSS.TransRecId                                   AS TransRecId,         SUM(CAST(VSS.ExChAdjustment AS DECIMAL(22,6)))   AS ExChAdjustment,         SUM(CAST(VSS.SettleAmountCur AS DECIMAL(22,6)))  AS SettleAmountCur,         SUM(CAST(VSS.SettleAmountMst AS DECIMAL(22,6)))  AS SettleAmountMst,         P.MonthId     FROM Period P     JOIN dbo.SMRBIVendSettlementStaging VSS         ON YEAR(VSS.TransDate) * 100 + MONTH(VSS.TransDate) <= P.MonthId     JOIN MaxSettlement         ON VSS.TransRecId = MaxSettlement.TransRecId         AND P.MonthId <= YEAR(MaxSettlement.MaxTransDate) * 100 + MONTH(MaxSettlement.MaxTransDate)     WHERE         P.YearId >= 2005         AND P.MonthId <= YEAR(date_sub(current_date(),1)) * 100 + MONTH(date_sub(current_date(),1))     GROUP BY         VSS.AccountNum,         VSS.TransCompany,         VSS.TransRecId,         P.MonthId,         P.PeriodDate )  SELECT     concat(VTS.VendTransRecId, VTS.DataAreaId)                      AS AccountsPayableCodeScreening,     VTS.VendTransRecId                                              AS RecId,     NVL(         CASE WHEN VTS.Invoice = '' THEN NULL ELSE upper(VTS.Invoice) END,         '_N/A'     )                                                               AS PurchaseInvoiceCode,     NVL(VTS.Voucher, '_N/A')                                        AS PayablesVoucher,     NVL(         CASE             WHEN LENGTH(TRIM(upper(VTS.Txt))) > 255             THEN CONCAT(SUBSTRING(TRIM(upper(VTS.Txt)), 1, 252), '...')             ELSE TRIM(upper(VTS.Txt))         END,         '_N/A'     )                                                               AS Description,     NVL(upper(VTS.DataAreaId), '_N/A')                              AS CompanyCode,     CASE         WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.MonthId              AND VTS.AmountCur = VSS.SettleAmountCur THEN 0         ELSE 1     END                                                            AS DimIsOpenAmountId,      -- Outstanding period calculation (converted DATEDIFF logic)     CASE         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 CASE                     WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                     ELSE VTS.DueDate                 END             ) <= 0 THEN '<0'         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 date_add(                     CASE                         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                         ELSE VTS.DueDate                     END,                     7                 )             ) <= 0 THEN '0-7'         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 date_add(                     CASE                         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                         ELSE VTS.DueDate                     END,                     15                 )             ) <= 0 THEN '8-15'         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 date_add(                     CASE                         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                         ELSE VTS.DueDate                     END,                     30                 )             ) <= 0 THEN '16-30'         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 date_add(                     CASE                         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                         ELSE VTS.DueDate                     END,                     60                 )             ) <= 0 THEN '31-60'         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 date_add(                     CASE                         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                         ELSE VTS.DueDate                     END,                     90                 )             ) <= 0 THEN '61-90'         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 date_add(                     CASE                         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                         ELSE VTS.DueDate                     END,                     120                 )             ) <= 0 THEN '91-120'         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 date_add(                     CASE                         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                         ELSE VTS.DueDate                     END,                     180                 )             ) <= 0 THEN '121-180'         WHEN datediff(                 CASE                     WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId                          AND VTS.AmountCur = VSS.SettleAmountCur                     THEN VSS.MaxTransDatePerPeriod                     ELSE P.PeriodDate                 END,                 date_add(                     CASE                         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate                         ELSE VTS.DueDate                     END,                     365                 )             ) <= 0 THEN '181-365'         ELSE '>365'     END                                                            AS OutStandingPeriodCode,      NVL(VTS.AccountNum, '_N/A')                                    AS SupplierCode,     upper(VTS.CurrencyCode)                                        AS TransactionCurrencyCode,     NVL(upper(L.AccountingCurrency), '_N/A')                       AS AccountingCurrencyCode,     NVL(upper(L.ReportingCurrency), '_N/A')                        AS ReportingCurrencyCode,     NVL(upper(L.GroupCurrency), '_N/A')                            AS GroupCurrencyCode,     NVL(VTS.TransDate, DATE('1900-01-01'))                         AS InvoiceDate,     CASE         WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate         WHEN VTS.DueDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DueDate         ELSE DATE('1900-01-01')     END                                                            AS DueDate,     NVL(VSS.MaxTransDatePerPeriod, DATE('1900-01-01'))             AS LastPaymentDate,     CASE         WHEN VTS.DocumentDate = DATE('1900-01-01') THEN DATE('1900-01-01')         WHEN VTS.DocumentDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DocumentDate         ELSE DATE('1900-01-01')     END                                                            AS DocumentDate,     P.DimPeriodId * 100 + 1                                        AS ReportDate,     CAST(VTS.AmountCur AS DECIMAL(22,6))                           AS InvoiceAmountTC,     CAST(VTS.AmountMst AS DECIMAL(22,6))                           AS InvoiceAmountAC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))             ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * RC.ExchangeRate         END,         0     )                                                               AS InvoiceAmountRC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))             ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * GC.ExchangeRate         END,         0     )                                                               AS InvoiceAmountGC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.AccountingCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))             ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * AC_Budget.ExchangeRate         END,         0     )                                                               AS InvoiceAmountAC_Budget,      NVL(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))             ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * RC_Budget.ExchangeRate         END,         0     )                                                               AS InvoiceAmountRC_Budget,      NVL(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency THEN CAST(VTS.AmountCur AS DECIMAL(22,6))             ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * GC_Budget.ExchangeRate         END,         0     )                                                               AS InvoiceAmountGC_Budget,      CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END AS PaidAmountTC,     CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS PaidAmountAC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency THEN                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * RC.ExchangeRate         END,         0     )                                                               AS PaidAmountRC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency THEN                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * GC.ExchangeRate         END,         0     )                                                               AS PaidAmountGC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.AccountingCurrency THEN                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate         END,         0     )                                                               AS PaidAmountAC_Budget,      NVL(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency THEN                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate         END,         0     )                                                               AS PaidAmountRC_Budget,      NVL(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency THEN                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate         END,         0     )                                                               AS PaidAmountGC_Budget,      CAST(VTS.AmountCur AS DECIMAL(22,6)) -         CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END AS OpenAmountTC,      CAST(VTS.AmountMst AS DECIMAL(22,6)) +         CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.ExChAdjustment END -         CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS OpenAmountAC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency THEN                 CAST(VTS.AmountCur AS DECIMAL(22,6)) -                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 (CAST(VTS.AmountCur AS DECIMAL(22,6)) -                  CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *                 RC.ExchangeRate         END,         0     )                                                               AS OpenAmountRC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency THEN                 CAST(VTS.AmountCur AS DECIMAL(22,6)) -                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 (CAST(VTS.AmountCur AS DECIMAL(22,6)) -                  CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *                 GC.ExchangeRate         END,         0     )                                                               AS OpenAmountGC,      NVL(         CASE             WHEN VTS.CurrencyCode = L.AccountingCurrency THEN                 CAST(VTS.AmountCur AS DECIMAL(22,6)) -                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 (CAST(VTS.AmountCur AS DECIMAL(22,6)) -                  CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *                 AC_Budget.ExchangeRate         END,         0     )                                                               AS OpenAmountAC_Budget,      NVL(         CASE             WHEN VTS.CurrencyCode = L.ReportingCurrency THEN                 CAST(VTS.AmountCur AS DECIMAL(22,6)) -                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 (CAST(VTS.AmountCur AS DECIMAL(22,6)) -                  CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *                 RC_Budget.ExchangeRate         END,         0     )                                                               AS OpenAmountRC_Budget,      NVL(         CASE             WHEN VTS.CurrencyCode = L.GroupCurrency THEN                 CAST(VTS.AmountCur AS DECIMAL(22,6)) -                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END             ELSE                 (CAST(VTS.AmountCur AS DECIMAL(22,6)) -                  CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *                 GC_Budget.ExchangeRate         END,         0     )                                                               AS OpenAmountGC_Budget,      CAST(1 AS DECIMAL(38,6))                                        AS AppliedExchangeRateTC,     NVL(RC.ExchangeRate, 0)                                         AS AppliedExchangeRateRC,     NVL(AC.ExchangeRate, 0)                                         AS AppliedExchangeRateAC,     NVL(GC.ExchangeRate, 0)                                         AS AppliedExchangeRateGC,     NVL(RC_Budget.ExchangeRate, 0)                                  AS AppliedExchangeRateRC_Budget,     NVL(AC_Budget.ExchangeRate, 0)                                  AS AppliedExchangeRateAC_Budget,     NVL(GC_Budget.ExchangeRate, 0)                                  AS AppliedExchangeRateGC_Budget  FROM dbo.SMRBIVendTransStaging VTS JOIN Period P     ON YEAR(VTS.TransDate) * 100 + MONTH(VTS.TransDate) <= P.MonthId     AND CASE             WHEN VTS.Closed = DATE('1900-01-01') THEN 999999             WHEN VTS.Closed < VTS.TransDate THEN YEAR(VTS.TransDate) * 100 + MONTH(VTS.TransDate)             ELSE YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)         END >= P.MonthId LEFT JOIN CumulSettlementsPerPeriod VSS     ON VTS.VendTransRecId = VSS.TransRecId     AND P.MonthId = VSS.MonthId  -- Lookup tables for currency exchange rates JOIN (     SELECT DISTINCT         LES.ReportingCurrency,         LES.AccountingCurrency,         LES.ExchangeRateType,         LES.BudgetExchangeRateType,         LES.Name,         G.GroupCurrencyCode AS GroupCurrency     FROM dbo.SMRBILedgerStaging LES     CROSS JOIN (SELECT GroupCurrencyCode FROM ETL.GroupCurrency LIMIT 1) G ) L     ON L.Name = VTS.DataAreaId  LEFT JOIN DataStore.ExchangeRate RC     ON RC.FromCurrencyCode = VTS.CurrencyCode     AND RC.ToCurrencyCode = L.ReportingCurrency     AND RC.ExchangeRateTypeCode = L.ExchangeRateType     AND VTS.TransDate BETWEEN RC.ValidFrom AND RC.ValidTo  LEFT JOIN DataStore.ExchangeRate AC     ON AC.FromCurrencyCode = VTS.CurrencyCode     AND AC.ToCurrencyCode = L.AccountingCurrency     AND AC.ExchangeRateTypeCode = L.ExchangeRateType     AND VTS.TransDate BETWEEN AC.ValidFrom AND AC.ValidTo  LEFT JOIN DataStore.ExchangeRate GC     ON GC.FromCurrencyCode = VTS.CurrencyCode     AND GC.ToCurrencyCode = L.GroupCurrency     AND GC.ExchangeRateTypeCode = L.ExchangeRateType     AND VTS.TransDate BETWEEN GC.ValidFrom AND GC.ValidTo  LEFT JOIN DataStore.ExchangeRate RC_Budget     ON RC_Budget.FromCurrencyCode = VTS.CurrencyCode     AND RC_Budget.ToCurrencyCode = L.ReportingCurrency     AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType     AND VTS.TransDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo  LEFT JOIN DataStore.ExchangeRate AC_Budget     ON AC_Budget.FromCurrencyCode = VTS.CurrencyCode     AND AC_Budget.ToCurrencyCode = L.AccountingCurrency     AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType     AND VTS.TransDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo  LEFT JOIN DataStore.ExchangeRate GC_Budget     ON GC_Budget.FromCurrencyCode = VTS.CurrencyCode     AND GC_Budget.ToCurrencyCode = L.GroupCurrency     AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType     AND VTS.TransDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo  CROSS JOIN (     SELECT         MIN(DateTime) AS MinDate,         MAX(DateTime) AS MaxDate     FROM ETL.Date     WHERE YearId > 1900 AND YearId < 9999 ) D  WHERE     P.YearId >= 2005     AND P.MonthId <= YEAR(date_sub(current_date(),1)) * 100 + MONTH(date_sub(current_date(),1))
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC ```
