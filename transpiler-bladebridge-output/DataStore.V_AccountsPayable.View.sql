/****** Object:  View [DataStore].[V_AccountsPayable]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_AccountsPayable` AS


WITH `Period` AS (
	SELECT MIN(MonthId) AS DimPeriodId
		 , MonthId
		 , MAX(TIMESTAMP) AS PeriodDate
		 , YearId
	FROM ETL.Date
	WHERE TIMESTAMP <= current_timestamp()
	GROUP BY MonthId, YEAR(TIMESTAMP), MONTH(TIMESTAMP), YearId
	),

	MaxSettlement AS (
	SELECT	VSS.TransRecId AS TransRecId
		  , CASE
			   WHEN VTS.Closed = '1900-01-01' THEN current_timestamp()
			   ELSE MAX(VSS.TransDate)
			END AS MaxTransDate
			/* When not Closed yet -> return numbers up until now */
	FROM dbo.SMRBIVendSettlementStaging VSS 
	JOIN dbo.SMRBIVendTransStaging VTS
	ON VSS.TransRecId = VTS.VendTransRecId
	GROUP BY VSS.TransRecId, VTS.Closed
	),

	CumulSettlementsPerPeriod AS (
	SELECT	  VSS.AccountNum AS AccountNum
			, VSS.TransCompany AS TransCompany
			, MAX(VSS.TransDate) AS MaxTransDatePerPeriod
			, VSS.TransRecId AS TransRecId
			, SUM(CAST(VSS.ExChAdjustment AS decimal(22,6))) AS ExChAdjustment
			, SUM(CAST(VSS.SettleAmountCur AS decimal(22,6))) AS SettleAmountCur
			, SUM(CAST(VSS.SettleAmountMst AS decimal(22,6))) AS SettleAmountMst
			, P.MonthId
	FROM `Period` P
	JOIN dbo.SMRBIVendSettlementStaging VSS
	ON YEAR(VSS.TransDate) * 100 + MONTH(VSS.TransDate) <= P.MonthId
	JOIN MaxSettlement 
	ON VSS.TransRecId = MaxSettlement.TransRecId 
	AND P.MonthId <= YEAR(MaxSettlement.MaxTransDate)*100 + MONTH(MaxSettlement.MaxTransDate)
	WHERE 1=1
	AND P.YearId >= 2005
	AND P.MonthId <= YEAR(current_timestamp() - INTERVAL 1 DAY) * 100 + MONTH(current_timestamp() - INTERVAL 1 DAY)
	GROUP BY VSS.AccountNum, VSS.TransCompany, VSS.TransRecId, P.MonthId, P.PeriodDate
	)

SELECT
		  --Information on fields
		  Concat(VTS.VendTransRecId,VTS.DataAreaId) AS AccountsPayableCodeScreening
		, VTS.VendTransRecId AS RecId
		, COALESCE(CASE WHEN VTS.Invoice = '' THEN NULL ELSE UPPER(VTS.Invoice) END, '_N/A') AS PurchaseInvoiceCode
		, COALESCE(VTS.Voucher, '_N/A') AS PayablesVoucher
		, COALESCE(NULLIF(CAST(CASE 
WHEN LEN(LTRIM(RTRIM(UPPER(VTS.Txt)))) > 255 
THEN LEFT(LTRIM(RTRIM(UPPER(VTS.Txt))), 255 - 3) || '...' 
ELSE LTRIM(RTRIM(UPPER(VTS.Txt))) 
END AS STRING), ''), '_N/A') AS `Description`
		  --Dimensions
		, COALESCE(UPPER(VTS.DataAreaId), '_N/A') AS CompanyCode
		, CASE 
			WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.MonthId AND VTS.AmountCur = VSS.SettleAmountCur THEN 0 
			ELSE 1 
		  END AS DimIsOpenAmountId
        , CASE
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '<0'
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '0-7'
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '8-15'
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '16-30'
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '31-60'
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '61-90'
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '91-120'
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '121-180'
            WHEN DATEDIFF($3, CASE WHEN (YEAR(VTS.Closed) * 100 + MONTH(VTS.Closed)) = P.DimPeriodId AND VTS.AmountCur = VSS.SettleAmountCur 
THEN VSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '181-365'
            ELSE '>365' 
        END AS OutStandingPeriodCode

		, COALESCE(VTS.AccountNum, '_N/A') AS SupplierCode
		, UPPER(VTS.CurrencyCode) AS TransactionCurrencyCode
		, COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), '_N/A') AS AccountingCurrencyCode
		, COALESCE(CAST(UPPER(L.ReportingCurrency) AS STRING), N'_N/A') AS ReportingCurrencyCode
		, COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode
		  --Dates
		, COALESCE(NULLIF(VTS.TransDate, ''), '1900-01-01') AS InvoiceDate
		, CASE 
			WHEN VTS.DueDate < VTS.TransDate THEN VTS.TransDate
			WHEN VTS.DueDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DueDate
			ELSE '1900-01-01'
			END AS DueDate
		, COALESCE(VSS.MaxTransDatePerPeriod, '1900-01-01') AS LastPaymentDate
		, CASE 
			WHEN VTS.DocumentDate = '1900-01-01' THEN '1900-01-01' --When Documentdate is unrealistically low or high -> 19000101
			WHEN VTS.DocumentDate BETWEEN D.MinDate AND D.MaxDate THEN VTS.DocumentDate
			ELSE '1900-01-01'
		  END AS DocumentDate
		, P.DimPeriodId * 100 || '01' AS ReportDate
		  --Measures
		, CAST(VTS.AmountCur AS decimal(22,6)) AS InvoiceAmountTC
		, CAST(VTS.AmountMst AS decimal(22,6)) InvoiceAmountAC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode  = L.ReportingCurrency 
THEN (CAST(VTS.AmountCur AS decimal(22, 6)))  
ELSE (CAST(VTS.AmountCur AS decimal(22, 6))) * RC.ExchangeRate 
END, 0) AS InvoiceAmountRC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode  = L.GroupCurrency 
THEN (CAST(VTS.AmountCur AS decimal(22, 6)))  
ELSE (CAST(VTS.AmountCur AS decimal(22, 6))) * GC.ExchangeRate 
END, 0) AS InvoiceAmountGC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.AccountingCurrency 
THEN (CAST(VTS.AmountCur AS decimal(22, 6)))  ELSE (CAST(VTS.AmountCur AS decimal(22, 6))) * AC_Budget.ExchangeRate 
END, 0) AS InvoiceAmountAC_Budget
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.ReportingCurrency 
THEN (CAST(VTS.AmountCur AS decimal(22, 6)))  ELSE (CAST(VTS.AmountCur AS decimal(22, 6))) * RC_Budget.ExchangeRate 
END, 0) AS InvoiceAmountRC_Budget
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.GroupCurrency 
THEN (CAST(VTS.AmountCur AS decimal(22, 6)))  ELSE (CAST(VTS.AmountCur AS decimal(22, 6))) * GC_Budget.ExchangeRate 
END, 0) AS InvoiceAmountGC_Budget
		, CASE 
			WHEN VSS.TransRecId IS NULL
			THEN 0.00 
			ELSE VSS.SettleAmountCur 
		  END AS PaidAmountTC
		, CASE
			WHEN VSS.TransRecId IS NULL
			THEN 0.00 
			ELSE VSS.SettleAmountMst 
		  END AS PaidAmountAC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.ReportingCurrency 
THEN (CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)  
ELSE (CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) * RC.ExchangeRate 
END, 0) AS PaidAmountRC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode  = L.GroupCurrency 
THEN (CASE WHEN VSS.TransRecId IS NULL 
THEN 0.00 ELSE VSS.SettleAmountCur END)  
ELSE (CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) * GC.ExchangeRate 
END, 0) AS PaidAmountGC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.AccountingCurrency THEN (CASE 
WHEN VSS.TransRecId IS NULL 
THEN 0.00 ELSE VSS.SettleAmountCur END)  
ELSE (CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) * AC_Budget.ExchangeRate 
END, 0) AS PaidAmountAC_Budget
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.ReportingCurrency THEN (CASE 
WHEN VSS.TransRecId IS NULL 
THEN 0.00 ELSE VSS.SettleAmountCur END)  
ELSE (CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) * AC_Budget.ExchangeRate 
END, 0) AS PaidAmountRC_Budget
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.GroupCurrency THEN (CASE 
WHEN VSS.TransRecId IS NULL 
THEN 0.00 ELSE VSS.SettleAmountCur END)  
ELSE (CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END) * AC_Budget.ExchangeRate 
END, 0) AS PaidAmountGC_Budget
		, CAST(VTS.AmountCur AS decimal(22,6)) 
		  - CASE
				WHEN VSS.TransRecId IS NULL 
				THEN 0.00 
				ELSE VSS.SettleAmountCur 
			END AS OpenAmountTC
		, CAST(VTS.AmountMst AS decimal(22,6)) 
		  + CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.ExChAdjustment END
		  - CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountMst END AS OpenAmountAC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.ReportingCurrency 
THEN (CAST(VTS.AmountCur AS decimal(22, 6)) - CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)
ELSE ((CAST(VTS.AmountCur AS decimal(22, 6))- CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)) * RC.ExchangeRate 
END, 0) AS OpenAmountRC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode  = L.GroupCurrency
THEN (CAST(VTS.AmountCur AS decimal(22, 6)) - CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)
ELSE ((CAST(VTS.AmountCur AS decimal(22, 6)) - CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)) * GC.ExchangeRate 
END, 0) AS OpenAmountGC
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.AccountingCurrency
THEN (CAST(VTS.AmountCur AS decimal(22, 6)) - CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)
ELSE ((CAST(VTS.AmountCur AS decimal(22, 6))- CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)) * AC_Budget.ExchangeRate 
END, 0) AS OpenAmountAC_Budget
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.ReportingCurrency
THEN (CAST(VTS.AmountCur AS decimal(22, 6)) - CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)
ELSE ((CAST(VTS.AmountCur AS decimal(22, 6))- CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)) * RC_Budget.ExchangeRate 
END, 0) AS OpenAmountRC_Budget
		, COALESCE(CASE 
WHEN VTS.CurrencyCode = L.GroupCurrency
THEN (CAST(VTS.AmountCur AS decimal(22, 6)) - CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)
ELSE ((CAST(VTS.AmountCur AS decimal(22, 6))- CASE WHEN VSS.TransRecId IS NULL THEN 0.00 ELSE VSS.SettleAmountCur END)) * GC_Budget.ExchangeRate 
END, 0) AS OpenAmountGC_Budget
		, CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
		, COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
		, COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
		, COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
		, COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
		, COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
		, COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget

FROM dbo.SMRBIVendTransStaging VTS 
INNER JOIN `Period` P  
ON YEAR(VTS.TransDate) * 100 + MONTH(VTS.TransDate) <= P.MonthId
AND CASE WHEN VTS.Closed = '1900-01-01' THEN 999999 
		 WHEN VTS.Closed < VTS.TransDate THEN (YEAR(VTS.TransDate)*100 + MONTH(VTS.TransDate))
		 ELSE (YEAR(VTS.Closed)*100 + MONTH(VTS.Closed)) END >= P.MonthId
LEFT JOIN CumulSettlementsPerPeriod VSS
ON VTS.VendTransRecId = VSS.TransRecId 
AND P.MonthId = VSS.MonthId

--Required for currencies
JOIN 
	(SELECT DISTINCT LES.ReportingCurrency
			       , LES.AccountingCurrency
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , LES.`Name`
			       , G.GroupCurrencyCode AS GroupCurrency
	 FROM dbo.SMRBILedgerStaging LES
	 CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON L.`Name` = VTS.DataAreaId

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurreny
ON RC.FromCurrencyCode = VTS.CurrencyCode 
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND VTS.TransDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = VTS.CurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND VTS.TransDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = VTS.CurrencyCode 
AND GC.ToCurrencyCode  = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND VTS.TransDate BETWEEN GC.ValidFrom AND GC.ValidTo
	
LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurreny
ON RC_Budget.FromCurrencyCode = VTS.CurrencyCode 
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND VTS.TransDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = VTS.CurrencyCode
AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND VTS.TransDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = VTS.CurrencyCode 
AND GC_Budget.ToCurrencyCode  = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND VTS.TransDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

--Required for ReportDate
CROSS JOIN 
	(SELECT MIN(TIMESTAMP) AS MinDate, MAX(TIMESTAMP) AS MaxDate 
	 FROM ETL.Date
	 WHERE YearId > 1900
		AND YearId < 9999) D

WHERE 1=1
	AND P.YearId >= 2005
	AND P.MonthId <= YEAR(current_timestamp() - INTERVAL 1 DAY) * 100 + MONTH(current_timestamp() - INTERVAL 1 DAY)
;
