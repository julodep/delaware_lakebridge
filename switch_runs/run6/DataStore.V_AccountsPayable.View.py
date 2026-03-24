# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_AccountsPayable.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324112609-alwp/DataStore.V_AccountsPayable.View.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Setup: import any required modules (Databricks provides `spark` by default)
# -------------------------------------------------------------------------

# -------------------------------------------------------------------------
# Drop the view if it already exists to avoid conflicts.
# -------------------------------------------------------------------------
spark.sql("DROP VIEW IF EXISTS dbe_dbx_internships.switchschema.V_AccountsPayable")

# COMMAND ----------

# -------------------------------------------------------------------------
# Create the view `V_AccountsPayable` in the target catalog and schema.
# The original T‑SQL has been rewritten to Spark SQL syntax.
# -------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE VIEW dbe_dbx_internships.switchschema.V_AccountsPayable AS
WITH PeriodCTE AS (
    SELECT
        MIN(MonthId) AS DimPeriodId,
        MonthId,
        MAX(DateTime) AS PeriodDate,
        YearId
    FROM dbe_dbx_internships.switchschema.Date
    WHERE DateTime <= current_timestamp()
    GROUP BY MonthId, year(DateTime), month(DateTime), YearId
),
MaxSettlement AS (
    SELECT
        VSS.TransRecId AS TransRecId,
        CASE
            WHEN VTS.Closed = DATE('1900-01-01') THEN current_timestamp()
            ELSE MAX(VSS.TransDate)
        END AS MaxTransDate
    FROM dbe_dbx_internships.switchschema.SMRBIVendSettlementStaging VSS
    JOIN dbe_dbx_internships.switchschema.SMRBIVendTransStaging VTS
      ON VSS.TransRecId = VTS.VendTransRecId
    GROUP BY VSS.TransRecId, VTS.Closed
),
CumulSettlementsPerPeriod AS (
    SELECT
        VSS.AccountNum AS AccountNum,
        VSS.TransCompany AS TransCompany,
        MAX(VSS.TransDate) AS MaxTransDatePerPeriod,
        VSS.TransRecId AS TransRecId,
        SUM(CAST(VSS.ExChAdjustment AS DECIMAL(22,6))) AS ExChAdjustment,
        SUM(CAST(VSS.SettleAmountCur AS DECIMAL(22,6))) AS SettleAmountCur,
        SUM(CAST(VSS.SettleAmountMst AS DECIMAL(22,6))) AS SettleAmountMst,
        P.MonthId
    FROM PeriodCTE P
    JOIN dbe_dbx_internships.switchschema.SMRBIVendSettlementStaging VSS
      ON year(VSS.TransDate) * 100 + month(VSS.TransDate) <= P.MonthId
    JOIN MaxSettlement
      ON VSS.TransRecId = MaxSettlement.TransRecId
     AND P.MonthId <= year(MaxSettlement.MaxTransDate) * 100 + month(MaxSettlement.MaxTransDate)
    WHERE P.YearId >= 2005
      AND P.MonthId <= year(date_sub(current_timestamp(), 1)) * 100 + month(date_sub(current_timestamp(), 1))
    GROUP BY VSS.AccountNum, VSS.TransCompany, VSS.TransRecId, P.MonthId, P.PeriodDate
),
DateBounds AS (
    SELECT
        MIN(DateTime) AS MinDate,
        MAX(DateTime) AS MaxDate
    FROM dbe_dbx_internships.switchschema.Date
    WHERE YearId > 1900 AND YearId < 9999
),
DefaultGroupCurrency AS (
    SELECT GroupCurrencyCode
    FROM dbe_dbx_internships.switchschema.GroupCurrency
    LIMIT 1
),
LedgerInfo AS (
    SELECT DISTINCT
        LES.ReportingCurrency,
        LES.AccountingCurrency,
        LES.ExchangeRateType,
        LES.BudgetExchangeRateType,
        LES.Name,
        G.GroupCurrencyCode AS GroupCurrency
    FROM dbe_dbx_internships.switchschema.SMRBILedgerStaging LES
    CROSS JOIN DefaultGroupCurrency G
)
SELECT
    concat(VTS.VendTransRecId, VTS.DataAreaId) AS AccountsPayableCodeScreening,
    VTS.VendTransRecId AS RecId,
    nvl(
        CASE WHEN trim(VTS.Invoice) = '' THEN NULL ELSE upper(VTS.Invoice) END,
        '_N/A'
    ) AS PurchaseInvoiceCode,
    nvl(VTS.Voucher, '_N/A') AS PayablesVoucher,
    nvl(
        NULLIF(
            CAST(
                CASE
                    WHEN length(trim(upper(VTS.Txt))) > 255
                    THEN concat(substr(trim(upper(VTS.Txt)), 1, 252), '...')
                    ELSE trim(upper(VTS.Txt))
                END AS STRING
            ),
            ''
        ),
        '_N/A'
    ) AS Description,
    nvl(upper(VTS.DataAreaId), '_N/A') AS CompanyCode,
    CASE
        WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.MonthId
             AND VTS.AmountCur = VSS.SettleAmountCur THEN 0
        ELSE 1
    END AS DimIsOpenAmountId,
    CASE
        WHEN datediff(
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END,
                CASE
                    WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                    ELSE VTS.DueDate
                END
            ) <= 0
        THEN '<0'
        WHEN datediff(
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END, 7),
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0
        THEN '0-7'
        WHEN datediff(
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END, 15),
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0
        THEN '8-15'
        WHEN datediff(
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END, 30),
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0
        THEN '16-30'
        WHEN datediff(
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END, 60),
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0
        THEN '31-60'
        WHEN datediff(
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END, 90),
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0
        THEN '61-90'
        WHEN datediff(
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END, 120),
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0
        THEN '91-120'
        WHEN datediff(
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END, 180),
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0
        THEN '121-180'
        WHEN datediff(
                date_add(
                    CASE
                        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
                        ELSE VTS.DueDate
                    END, 365),
                CASE
                    WHEN (year(VTS.Closed) * 100 + month(VTS.Closed)) = P.DimPeriodId
                         AND VTS.AmountCur = VSS.SettleAmountCur
                    THEN VSS.MaxTransDatePerPeriod
                    ELSE P.PeriodDate
                END
            ) <= 0
        THEN '181-365'
        ELSE '>365'
    END AS OutStandingPeriodCode,
    nvl(VTS.AccountNum, '_N/A') AS SupplierCode,
    upper(VTS.CurrencyCode) AS TransactionCurrencyCode,
    nvl(upper(L.AccountingCurrency), '_N/A') AS AccountingCurrencyCode,
    nvl(upper(L.ReportingCurrency), '_N/A') AS ReportingCurrencyCode,
    nvl(upper(L.GroupCurrency), '_N/A') AS GroupCurrencyCode,
    nvl(VTS.TransDate, DATE('1900-01-01')) AS InvoiceDate,
    CASE
        WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
        WHEN VTS.DueDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DueDate
        ELSE DATE('1900-01-01')
    END AS DueDate,
    nvl(VSS.MaxTransDatePerPeriod, DATE('1900-01-01')) AS LastPaymentDate,
    CASE
        WHEN VTS.DocumentDate = DATE('1900-01-01') THEN DATE('1900-01-01')
        WHEN VTS.DocumentDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DocumentDate
        ELSE DATE('1900-01-01')
    END AS DocumentDate,
    P.DimPeriodId * 100 + 1 AS ReportDate,
    CAST(VTS.AmountCur AS DECIMAL(22,6)) AS InvoiceAmountTC,
    CAST(VTS.AmountMst AS DECIMAL(22,6)) AS InvoiceAmountAC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * RC.ExchangeRate
        END,
        0
    ) AS InvoiceAmountRC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * GC.ExchangeRate
        END,
        0
    ) AS InvoiceAmountGC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.AccountingCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * AC_Budget.ExchangeRate
        END,
        0
    ) AS InvoiceAmountAC_Budget,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * RC_Budget.ExchangeRate
        END,
        0
    ) AS InvoiceAmountRC_Budget,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6))
            ELSE CAST(VTS.AmountCur AS DECIMAL(22,6)) * GC_Budget.ExchangeRate
        END,
        0
    ) AS InvoiceAmountGC_Budget,
    CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END AS PaidAmountTC,
    CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS PaidAmountAC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency
                THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * RC.ExchangeRate
        END,
        0
    ) AS PaidAmountRC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency
                THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * GC.ExchangeRate
        END,
        0
    ) AS PaidAmountGC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.AccountingCurrency
                THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate
        END,
        0
    ) AS PaidAmountAC_Budget,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency
                THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate
        END,
        0
    ) AS PaidAmountRC_Budget,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency
                THEN CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END * AC_Budget.ExchangeRate
        END,
        0
    ) AS PaidAmountGC_Budget,
    CAST(VTS.AmountCur AS DECIMAL(22,6)) -
        CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END AS OpenAmountTC,
    CAST(VTS.AmountMst AS DECIMAL(22,6)) +
        CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.ExChAdjustment END -
        CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS OpenAmountAC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                     CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                 RC.ExchangeRate
        END,
        0
    ) AS OpenAmountRC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                     CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                 GC.ExchangeRate
        END,
        0
    ) AS OpenAmountGC,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.AccountingCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                     CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                 AC_Budget.ExchangeRate
        END,
        0
    ) AS OpenAmountAC_Budget,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.ReportingCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                     CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                 RC_Budget.ExchangeRate
        END,
        0
    ) AS OpenAmountRC_Budget,
    nvl(
        CASE
            WHEN VTS.CurrencyCode = L.GroupCurrency
                THEN CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                     CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END
            ELSE (CAST(VTS.AmountCur AS DECIMAL(22,6)) -
                 CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) *
                 GC_Budget.ExchangeRate
        END,
        0
    ) AS OpenAmountGC_Budget,
    CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC,
    nvl(RC.ExchangeRate, 0) AS AppliedExchangeRateRC,
    nvl(AC.ExchangeRate, 0) AS AppliedExchangeRateAC,
    nvl(GC.ExchangeRate, 0) AS AppliedExchangeRateGC,
    nvl(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget,
    nvl(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget,
    nvl(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget
FROM dbe_dbx_internships.switchschema.SMRBIVendTransStaging VTS
JOIN PeriodCTE P
  ON year(VTS.TransDate) * 100 + month(VTS.TransDate) <= P.MonthId
 AND CASE
        WHEN VTS.Closed = DATE('1900-01-01') THEN 999999
        WHEN VTS.Closed < VTS.TransDate THEN year(VTS.TransDate) * 100 + month(VTS.TransDate)
        ELSE year(VTS.Closed) * 100 + month(VTS.Closed)
    END >= P.MonthId
LEFT JOIN CumulSettlementsPerPeriod VSS
  ON VTS.VendTransRecId = VSS.TransRecId
 AND P.MonthId = VSS.MonthId
JOIN LedgerInfo L
  ON L.Name = VTS.DataAreaId
LEFT JOIN dbe_dbx_internships.switchschema.ExchangeRate RC
  ON RC.FromCurrencyCode = VTS.CurrencyCode
 AND RC.ToCurrencyCode = L.ReportingCurrency
 AND RC.ExchangeRateTypeCode = L.ExchangeRateType
 AND VTS.TransDate BETWEEN RC.ValidFrom AND RC.ValidTo
LEFT JOIN dbe_dbx_internships.switchschema.ExchangeRate AC
  ON AC.FromCurrencyCode = VTS.CurrencyCode
 AND AC.ToCurrencyCode = L.AccountingCurrency
 AND AC.ExchangeRateTypeCode = L.ExchangeRateType
 AND VTS.TransDate BETWEEN AC.ValidFrom AND AC.ValidTo
LEFT JOIN dbe_dbx_internships.switchschema.ExchangeRate GC
  ON GC.FromCurrencyCode = VTS.CurrencyCode
 AND GC.ToCurrencyCode = L.GroupCurrency
 AND GC.ExchangeRateTypeCode = L.ExchangeRateType
 AND VTS.TransDate BETWEEN GC.ValidFrom AND GC.ValidTo
LEFT JOIN dbe_dbx_internships.switchschema.ExchangeRate RC_Budget
  ON RC_Budget.FromCurrencyCode = VTS.CurrencyCode
 AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
 AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
 AND VTS.TransDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo
LEFT JOIN dbe_dbx_internships.switchschema.ExchangeRate AC_Budget
  ON AC_Budget.FromCurrencyCode = VTS.CurrencyCode
 AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
 AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
 AND VTS.TransDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo
LEFT JOIN dbe_dbx_internships.switchschema.ExchangeRate GC_Budget
  ON GC_Budget.FromCurrencyCode = VTS.CurrencyCode
 AND GC_Budget.ToCurrencyCode = L.GroupCurrency
 AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
 AND VTS.TransDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo
CROSS JOIN DateBounds D
WHERE P.YearId >= 2005
  AND P.MonthId <= year(date_sub(current_timestamp(), 1)) * 100 + month(date_sub(current_timestamp(), 1))
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
