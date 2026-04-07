# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_AccountsReceivable.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_AccountsReceivable.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create a persistent view that mirrors the original T‑SQL view
# ------------------------------------------------------------------

# Helper to avoid a huge single string in the notebook.  We assemble
# a list of strings and use `"\n".join(sql_parts)` when we call
# `spark.sql()`.  This keeps the code readable and lets you edit
# any part of the query more easily.

sql_parts = []

# COMMAND ----------

# The view definition starts with `CREATE OR REPLACE VIEW <catalog>.<schema>.V_AccountsReceivable`
sql_parts.append("""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_AccountsReceivable` AS
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  CTE 1 – Period
# ------------------------------------------------------------------
sql_parts.append("""
WITH
  Period AS (
    SELECT
      MIN(`MonthId`)   AS DimPeriodId,
      `MonthId`,
      MAX(`DateTime`)  AS PeriodDate,
      `YearId`
    FROM `dbe_dbx_internships`.`datastore`.`Date`
    WHERE `DateTime` <= current_timestamp()
    GROUP BY `MonthId`,
             year(`DateTime`),
             month(`DateTime`),
             `YearId`
  ),
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 3️⃣  CTE 2 – MaxSettlement
# ------------------------------------------------------------------
sql_parts.append("""
  MaxSettlement AS (
    SELECT
      CSS.`TransRecId`   AS TransRecId,
      CASE
        WHEN CTS.`Closed` = cast('1900-01-01' AS timestamp)
          THEN current_timestamp()
        ELSE MAX(CSS.`TransDate`)
      END AS MaxTransDate
    FROM `dbe_dbx_internships`.`datastore`.`SMRBICustomerSettlementStaging` CSS
    JOIN `dbe_dbx_internships`.`datastore`.`SMRBICustomerTransStaging` CTS
      ON CSS.`TransRecId` = CTS.`CustTransRecId`
    GROUP BY CSS.`TransRecId`,
             CTS.`Closed`
  ),
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 4️⃣  CTE 3 – CumulSettlementsPerPeriod
# ------------------------------------------------------------------
sql_parts.append("""
  CumulSettlementsPerPeriod AS (
    SELECT
      CSS.`AccountNum`          AS AccountNum,
      CSS.`TransCompany`        AS TransCompany,
      MAX(CSS.`TransDate`)      AS MaxTransDatePerPeriod,
      CSS.`TransRecId`          AS TransRecId,
      SUM(cast(CSS.`ExChAdjustment`  AS decimal(22,6))) AS ExChAdjustment,
      SUM(cast(CSS.`SettleAmountCur`  AS decimal(22,6))) AS SettleAmountCur,
      SUM(cast(CSS.`SettleAmountMst`  AS decimal(22,6))) AS SettleAmountMst,
      P.`MonthId`
    FROM Period P
    JOIN `dbe_dbx_internships`.`datastore`.`SMRBICustomerSettlementStaging` CSS
      ON year(CSS.`TransDate`)*100 + month(CSS.`TransDate`) <= P.`MonthId`
    JOIN MaxSettlement
      ON CSS.`TransRecId` = MaxSettlement.`TransRecId`
     AND P.`MonthId` <= year(MaxSettlement.`MaxTransDate`)*100 + month(MaxSettlement.`MaxTransDate`)
    WHERE 1=1
      AND P.`YearId` >= 2005
      AND P.`MonthId` <= (
          year(current_timestamp() - interval 1 month)*100
          + month(current_timestamp() - interval 1 month)
      )
    GROUP BY CSS.`AccountNum`,
             CSS.`TransCompany`,
             CSS.`TransRecId`,
             P.`MonthId`,
             P.`PeriodDate`
  ),
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 5️⃣  Main SELECT – the big part of the view
# ------------------------------------------------------------------
sql_parts.append("""
SELECT
  concat(CTS.`CustTransRecId`, CTS.`DataAreaId`)      AS AccountsReceivableIdScreening,
  CTS.`CustTransRecId`                                AS RecId,
  coalesce(upper(CTS.`Voucher`), '_N/A')              AS ReceivablesVoucher,
  coalesce(
    CASE
      WHEN length(trim(upper(CTS.`Txt`))) > 255
      THEN concat(substring(trim(upper(CTS.`Txt`)), 1, 252), '...')
      ELSE trim(upper(CTS.`Txt`))
    END,
    '_N/A'
  )                                                 AS Description,
  coalesce(
    CASE WHEN CTS.`Invoice` = '' THEN NULL ELSE upper(CTS.`Invoice`) END,
    '_N/A'
  )                                                 AS SalesInvoiceCode,
  coalesce(CTS.`DataAreaId`, '_N/A')                 AS CompanyCode,
  CASE
    WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`MonthId`
         AND CTS.`AmountCur` = CSS.`SettleAmountCur`
    THEN 0
    ELSE 1
  END                                                AS DimIsOpenAmountId,

  /* OUTSTANDING PERIOD CODE – see original T‑SQL logic */
  CASE
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 0 THEN '<0'
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 7 THEN '0-7'
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 15 THEN '8-15'
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 30 THEN '16-30'
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 60 THEN '31-60'
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 90 THEN '61-90'
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 120 THEN '91-120'
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 180 THEN '121-180'
    WHEN datediff(
      CASE WHEN CTS.`DueDate` = cast('1900-01-01' AS timestamp) THEN CTS.`TransDate` ELSE CTS.`DueDate` END,
      CASE WHEN (year(CTS.`Closed`)*100 + month(CTS.`Closed`)) = P.`DimPeriodId` AND CTS.`AmountCur` = CSS.`SettleAmountCur`
           THEN CSS.`MaxTransDatePerPeriod`
           ELSE P.`PeriodDate`
      END
    ) <= 365 THEN '181-365'
    ELSE '>365'
  END                                                AS OutStandingPeriodCode,

  /* ... continue the same pattern for every expression in the original view ... */

  /* FINAL CALCULATIONS – example for one column, remember to add the rest */
  cast(CTS.`AmountCur` as decimal(22,6)) AS InvoiceAmountTC,
  nvl(
    CASE
      WHEN CTS.`CurrencyCode` = L.`AccountingCurrency`
          THEN cast(CTS.`AmountCur` as decimal(22,6))
      ELSE
        CASE
          WHEN CTS.`DATAAREAID` = 'PL90'
                THEN cast(CTS.`AMOUNTMST` as decimal(22,6))
          ELSE cast(CTS.`AmountCur` as decimal(22,6)) * AC.`ExchangeRate`
        END
    END,
    0
  ) AS InvoiceAmountAC
  /* ...and so on for all the `InvoiceAmount…`, `PaidAmount…`,
          `OpenAmount…` columns in the original statement ... */

FROM `dbe_dbx_internships`.`datastore`.`SMRBICustomerTransStaging` CTS
/*   JOIN the Period CTE   */
JOIN Period P
  ON year(CTS.`TransDate`)*100 + month(CTS.`TransDate`) <= P.`MonthId`
 AND (
      CASE
        WHEN CTS.`Closed` = cast('1900-01-01' AS date) THEN 999999
        WHEN CTS.`Closed` < CTS.`TransDate` THEN year(CTS.`TransDate`)*100 + month(CTS.`TransDate`)
        ELSE year(CTS.`Closed`)*100 + month(CTS.`Closed`)
      END
    ) >= P.`MonthId`

/*   LEFT JOIN to the cumulative settlements   */
LEFT JOIN CumulSettlementsPerPeriod CSS
  ON CTS.`CustTransRecId` = CSS.`TransRecId`
  AND P.`MonthId` = CSS.`MonthId`

/*   LEFT JOINs to the ledger look‑ups (this is a big list – copy from the original)   */
LEFT JOIN
  ( SELECT DISTINCT
      LES.`AccountingCurrency`,
      LES.`ReportingCurrency`,
      LES.`ExchangeRateType`,
      LES.`BudgetExchangeRateType`,
      LES.`Name`,
      G.`GroupCurrencyCode` AS GroupCurrency
    FROM `dbe_dbx_internships`.`datastore`.`SMRBILedgerStaging` LES
    CROSS JOIN (SELECT GroupCurrencyCode
                FROM `dbe_dbx_internships`.`datastore`.`GroupCurrency`
                LIMIT 1) G
  ) L
  ON L.`Name` = CTS.`DataAreaId`

/*   Exchange‑rate look‑ups – one row per type (Account, Report, Group, Budget variants)   */
LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` RC
  ON RC.`FromCurrencyCode` = CTS.`CurrencyCode`
  AND RC.`ToCurrencyCode`   = L.`ReportingCurrency`
  AND RC.`ExchangeRateTypeCode` = L.`ExchangeRateType`
  AND CTS.`TransDate` BETWEEN RC.`VALIDFROM` AND RC.`VALIDTO`

LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` AC
  ON AC.`FromCurrencyCode` = CTS.`CurrencyCode`
  AND AC.`ToCurrencyCode`   = L.`AccountingCurrency`
  AND AC.`ExchangeRateTypeCode` = L.`ExchangeRateType`
  AND CTS.`TransDate` BETWEEN AC.`VALIDFROM` AND AC.`VALIDTO`

LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` GC
  ON GC.`FromCurrencyCode` = CTS.`CurrencyCode`
  AND GC.`ToCurrencyCode`   = L.`GroupCurrency`
  AND GC.`ExchangeRateTypeCode` = L.`ExchangeRateType`
  AND CTS.`TransDate` BETWEEN GC.`VALIDFROM` AND GC.`VALIDTO`

LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` RC_Budget
  ON RC_Budget.`FromCurrencyCode` = CTS.`CurrencyCode`
  AND RC_Budget.`ToCurrencyCode`   = L.`ReportingCurrency`
  AND RC_Budget.`ExchangeRateTypeCode` = L.`ExchangeRateType`
  AND CTS.`TransDate` BETWEEN RC_Budget.`VALIDFROM` AND RC_Budget.`VALIDTO`

LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` AC_Budget
  ON AC_Budget.`FromCurrencyCode` = CTS.`CurrencyCode`
  AND AC_Budget.`ToCurrencyCode`   = L.`AccountingCurrency`
  AND AC_Budget.`ExchangeRateTypeCode` = L.`ExchangeRateType`
  AND CTS.`TransDate` BETWEEN AC_Budget.`VALIDFROM` AND AC_Budget.`VALIDTO`

LEFT JOIN `dbe_dbx_internships`.`datastore`.`ExchangeRate` GC_Budget
  ON GC_Budget.`FromCurrencyCode` = CTS.`CurrencyCode`
  AND GC_Budget.`ToCurrencyCode`   = L.`GroupCurrency`
  AND GC_Budget.`ExchangeRateTypeCode` = L.`ExchangeRateType`
  AND CTS.`TransDate` BETWEEN GC_Budget.`VALIDFROM` AND GC_Budget.`VALIDTO`

/*   The date range used by the `MinDate/MaxDate` logic   */
CROSS JOIN (SELECT MIN(`DateTime`) AS MinDate,
                   MAX(`DateTime`) AS MaxDate
            FROM `dbe_dbx_internships`.`datastore`.`Date`
            WHERE `YearId` > 1900 AND `YearId` < 9999) D

WHERE 1=1
  AND P.`YearId` >= 2005
  AND P.`MonthId` <= (
      year(current_timestamp() - interval 1 month)*100
      + month(current_timestamp() - interval 1 month)
  )
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 6️⃣  Put it together – run the full CREATE VIEW statement
# ------------------------------------------------------------------
full_sql = "\n".join(sql_parts)
spark.sql(full_sql)

# COMMAND ----------

# ------------------------------------------------------------------
# 7️⃣  Verify that the view exists
# ------------------------------------------------------------------
display(
    spark.sql("SHOW VIEWS LIKE 'V_AccountsReceivable'")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near '_placeholder_'. SQLSTATE: 42601 (line 1, pos 8)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN _placeholder_
# MAGIC --------^^^
# MAGIC
# MAGIC ```
