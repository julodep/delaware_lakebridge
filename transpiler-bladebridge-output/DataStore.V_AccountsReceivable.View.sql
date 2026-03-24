/****** Object:  View [DataStore].[V_AccountsReceivable]    Script Date: 03/03/2026 16:26:09 ******/















CREATE OR REPLACE VIEW `DataStore`.`V_AccountsReceivable` AS


WITH `Period` AS (
	SELECT MIN(MonthId) AS DimPeriodId
		 , MonthId
		 , MAX(TIMESTAMP) AS PeriodDate
		 , YearId
	FROM ETL.Date
	WHERE TIMESTAMP <= current_timestamp()
	GROUP BY MonthId, YEAR(TIMESTAMP), MONTH(TIMESTAMP), YearId
	)

	--Return the latest settlement date for a particular customer transaction (if applicable)
	, MaxSettlement AS (
	SELECT CSS.TransRecId AS TransRecId
		 , CASE
				WHEN CTS.Closed = '1900-01-01' THEN current_timestamp()
				ELSE MAX(CSS.TransDate) 
		   END AS MaxTransDate
		   /* When not Closed yet -> return numbers up until now */
	FROM dbo.SMRBICustomerSettlementStaging CSS 
	JOIN dbo.SMRBICustomerTransStaging CTS
	ON CSS.TransRecId = CTS.CustTransRecId
	GROUP BY CSS.TransRecId, CTS.Closed
	)
	
	--Return the cumulative settlements per account and company, for a particular month
	, CumulSettlementsPerPeriod AS (
	  SELECT  CSS.AccountNum AS AccountNum
			, CSS.TransCompany AS TransCompany
			, MAX(CSS.TransDate) AS MaxTransDatePerPeriod
			, CSS.TransRecId AS TransRecId
			, SUM(CAST(CSS.ExChAdjustment AS DECIMAL(22,6))) AS ExChAdjustment
			, SUM(CAST(CSS.SettleAmountCur AS DECIMAL(22,6))) AS SettleAmountCur
			, SUM(CAST(CSS.SettleAmountMst AS DECIMAL(22,6))) AS SettleAmountMst
			, P.MonthId
	FROM `Period` P
	JOIN dbo.SMRBICustomerSettlementStaging CSS
	ON YEAR(CSS.TransDate)*100 + MONTH(CSS.TransDate) <= P.MonthId
	JOIN MaxSettlement 
	ON CSS.TransRecId = MaxSettlement.TransRecId 
	AND P.MonthId <= YEAR(MaxSettlement.MaxTransDate)*100 + MONTH(MaxSettlement.MaxTransDate)
	WHERE 1=1
	AND P.YearId >= 2005
	AND P.MonthId <= YEAR(current_timestamp() - INTERVAL 1 DAY) * 100 + MONTH(current_timestamp() - INTERVAL 1 DAY)
	GROUP BY CSS.AccountNum, CSS.TransCompany, CSS.TransRecId, P.MonthId, P.PeriodDate
	)

SELECT --Information on fields
	   Concat(CTS.CustTransRecId,CTS.DataAreaId) AS AccountsReceivableIdScreening
	 , CTS.CustTransRecId AS RecId
	 , COALESCE(UPPER(CTS.Voucher), '_N/A') AS ReceivablesVoucher
	 , COALESCE(NULLIF(CAST(CASE WHEN LEN(LTRIM(RTRIM(UPPER(CTS.Txt)))) > 255 THEN LEFT(LTRIM(RTRIM(UPPER(CTS.Txt))), 255 - 3) || '...' 
ELSE LTRIM(RTRIM(UPPER(CTS.Txt))) 
END AS STRING), ''), '_N/A') AS `Description`
	 , COALESCE(CASE WHEN CTS.Invoice = '' THEN NULL ELSE UPPER(CTS.Invoice) END, '_N/A') AS SalesInvoiceCode
	   --Dimensions
	 , COALESCE(CTS.DataAreaId, '_N/A') AS CompanyCode
	 , CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.MonthId AND CTS.AmountCur = CSS.SettleAmountCur THEN 0 
	 		ELSE 1 
	   END AS DimIsOpenAmountId
	 , CASE
		WHEN DATEDIFF(DAY, DATEADD(DAY,  0, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE P.PeriodDate END) <= 0 THEN  '<0'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  7, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '0-7'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  15, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '8-15'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  30, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '16-30'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  60, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '31-60'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  90, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '61-90'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  120, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '91-120'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  120, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '91-120'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  180, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '121-180'
		WHEN DATEDIFF(DAY, DATEADD(DAY,  365, CASE WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate ELSE
CTS.DueDate ;
END), 
											CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END) <= 0 THEN  '181-365'
		ELSE
'>365' 
		;
END AS OutStandingPeriodCode


	, CASE WHEN (YEAR(CTS.Closed) * 100 + MONTH(CTS.Closed)) = P.DimPeriodId AND CTS.AmountCur = CSS.SettleAmountCur 
											THEN CSS.MaxTransDatePerPeriod ELSE
P.PeriodDate ;
END AS Test




	 , UPPER(COALESCE(CTS.AccountNum, '_N/A')) AS CustomerCode
	 , L.ExchangeRateType AS DefaultExchangeRateTypeCode
	 , L.BudgetExchangeRateType AS BudgetExchangeRateTypeCode
	 , CTS.CurrencyCode AS TransactionCurrencyCode
	 , COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), N'_N/A') AS AccountingCurrencyCode
	 , COALESCE(CAST(UPPER(L.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
	 , COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode
	   --Dates
	 , CTS.TransDate AS InvoiceDate
------------
	 , CASE 
	 		WHEN CTS.DueDate = '1900-01-01' THEN CTS.TransDate
	 		WHEN CTS.DueDate BETWEEN D.MinDate AND D.MaxDate THEN CTS.DueDate
	 		ELSE '1900-01-01'
	   END AS DueDate
------------
	 , COALESCE(CSS.MaxTransDatePerPeriod, '1900-01-01') AS LastPaymentDate
	 , CASE
	 		WHEN CTS.DocumentDate = '1900-01-01' THEN '1900-01-01' --When Documentdate is unrealistically low or high -> 19000101
	 		WHEN CTS.DocumentDate BETWEEN D.MinDate AND D.MaxDate THEN CTS.DocumentDate
	 		ELSE '1900-01-01'
	   END AS DocumentDate
	 , P.DimPeriodId * 100 || '01' AS ReportDate
	   --Measures
	 , CAST(CTS.AmountCur AS DECIMAL(22,6)) AS InvoiceAmountTC
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.AccountingCurrency
THEN CAST(CTS.AmountCur AS DECIMAL(22, 6))  
ELSE CASE WHEN CTS.DATAAREAID = 'PL90' 
THEN CAST(CTS.AMOUNTMST AS DECIMAL(22, 6)) 
else CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
* AC.ExchangeRate 
END
END, 0) AS InvoiceAmountAC
     --, ISNULL(CASE WHEN CTS.CurrencyCode  = L.AccountingCurrency
				 --  THEN CONVERT(DECIMAL(22, 6), CTS.AmountCur)  
				 --  ELSE CONVERT(DECIMAL(22, 6), CTS.AmountCur) 
					--	* AC.ExchangeRate END,0) AS InvoiceAmountAC 
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.ReportingCurrency 
THEN CAST(CTS.AmountCur AS DECIMAL(22, 6))  
ELSE CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
* RC.ExchangeRate END, 0) AS InvoiceAmountRC
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.GroupCurrency 
THEN CAST(CTS.AmountCur AS DECIMAL(22, 6))  
ELSE CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
* GC.ExchangeRate END, 0) AS InvoiceAmountGC	 
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.AccountingCurrency 
THEN CAST(CTS.AmountCur AS DECIMAL(22, 6))  
ELSE CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
* AC_Budget.ExchangeRate END, 0) AS InvoiceAmountAC_Budget
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.ReportingCurrency 
THEN CAST(CTS.AmountCur AS DECIMAL(22, 6))  
ELSE CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
* RC_Budget.ExchangeRate END, 0) AS InvoiceAmountRC_Budget
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.GroupCurrency 
THEN CAST(CTS.AmountCur AS DECIMAL(22, 6))  
ELSE CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
* GC_Budget.ExchangeRate END, 0) AS InvoiceAmountGC_Budget	 
	 , CASE 
	 		WHEN CSS.TransRecId IS NULL 
	 		THEN 0.00
	 		ELSE CSS.SettleAmountCur 
	   END AS PaidAmountTC
	 , COALESCE()(CASE WHEN CTS.CurrencyCode  = L.AccountingCurrency 
				   THEN (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)  
				   ELSE
(CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE
CSS.SettleAmountCur ;
END ) 
						* AC.ExchangeRate ;
END,0) AS PaidAmountAC
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.ReportingCurrency 
THEN (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)  
ELSE (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END ) 
* RC.ExchangeRate END, 0) AS PaidAmountRC
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.GroupCurrency 
THEN (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END )  
ELSE (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END  ) 
* GC.ExchangeRate END, 0) AS PaidAmountGC
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.AccountingCurrency 
THEN (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)  
ELSE (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END ) 
* AC_Budget.ExchangeRate END, 0) AS PaidAmountAC_Budget
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.ReportingCurrency 
THEN (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)  
ELSE (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END ) 
* RC_Budget.ExchangeRate END, 0) AS PaidAmountRC_Budget
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.GroupCurrency 
THEN (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)  
ELSE (CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END ) 
* GC_Budget.ExchangeRate END, 0) AS PaidAmountGC_Budget	 
	 , CAST(CTS.AmountCur AS DECIMAL(22,6))
	   - CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE
CSS.SettleAmountCur ;
END AS OpenAmountTC
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.AccountingCurrency
THEN (CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)
ELSE ((CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)) 
* AC.ExchangeRate END, 0) AS OpenAmountAC
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.ReportingCurrency 
THEN (CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)
ELSE ((CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)) 
* RC.ExchangeRate END, 0) AS OpenAmountRC
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.GroupCurrency
THEN (CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)
ELSE ((CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)) 
* GC.ExchangeRate END, 0) AS OpenAmountGC 
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.AccountingCurrency 
THEN (CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)  
ELSE ((CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)) 
* AC_Budget.ExchangeRate END, 0) AS OpenAmountAC_Budget
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.ReportingCurrency 
THEN (CAST(CTS.AmountCur AS DECIMAL(22, 6)) 
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END) 
ELSE ((CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)) 
* RC_Budget.ExchangeRate END, 0) AS OpenAmountRC_Budget
	 , COALESCE(CASE WHEN CTS.CurrencyCode  = L.GroupCurrency 
THEN (CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)  
ELSE ((CAST(CTS.AmountCur AS DECIMAL(22, 6))
- CASE WHEN CSS.TransRecId IS NULL THEN 0.00 ELSE CSS.SettleAmountCur END)) 
* GC_Budget.ExchangeRate END, 0) AS OpenAmountGC_Budget	 
	 , CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
	 , COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
	 , COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
	 , COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
	 , COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
	 , COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
	 , COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget

	   
FROM dbo.SMRBICustomerTransStaging CTS 
JOIN Period P  
ON YEAR(CTS.TransDate)*100 + MONTH(CTS.TransDate) <= P.MonthId
AND CASE 
		WHEN CTS.Closed = '1900-01-01' THEN 999999 
		WHEN CTS.Closed < CTS.TransDate THEN (YEAR(CTS.TransDate)*100 + MONTH(CTS.TransDate))
		ELSE (YEAR(CTS.Closed)*100 + MONTH(CTS.Closed))
		END >= P.MonthId

LEFT JOIN CumulSettlementsPerPeriod CSS
ON CTS.CustTransRecId = CSS.TransRecId 
AND P.MonthId = CSS.MonthId

--Required for currencies
JOIN 
	(SELECT DISTINCT LES.AccountingCurrency
			, LES.ReportingCurrency
			, LES.ExchangeRateType
			, LES.BudgetExchangeRateType
			, LES.`Name`
			, G.GroupCurrencyCode AS GroupCurrency
	 FROM dbo.SMRBILedgerStaging LES
	 CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON L.`Name` = CTS.DataAreaId

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrenycy
ON RC.FromCurrencyCode = CTS.CurrencyCode 
AND RC.ToCurrencyCode  = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND CTS.TransDate BETWEEN RC.VALIDFROM AND RC.VALIDTO

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = CTS.CurrencyCode
AND AC.ToCurrencyCode  = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND CTS.TransDate BETWEEN AC.VALIDFROM AND AC.VALIDTO

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = CTS.CurrencyCode 
AND GC.ToCurrencyCode  = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND CTS.TransDate BETWEEN GC.VALIDFROM AND GC.VALIDTO

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrenycy
ON RC_Budget.FromCurrencyCode = CTS.CurrencyCode 
AND RC_Budget.ToCurrencyCode  = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.ExchangeRateType
AND CTS.TransDate BETWEEN RC_Budget.VALIDFROM AND RC_Budget.VALIDTO

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = CTS.CurrencyCode
AND AC_Budget.ToCurrencyCode  = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.ExchangeRateType
AND CTS.TransDate BETWEEN AC_Budget.VALIDFROM AND AC_Budget.VALIDTO

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = CTS.CurrencyCode 
AND GC_Budget.ToCurrencyCode  = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.ExchangeRateType
AND CTS.TransDate BETWEEN GC_Budget.VALIDFROM AND GC_Budget.VALIDTO

--Required for ReportDate
CROSS JOIN 
	(SELECT MIN(TIMESTAMP) AS MinDate
			, MAX(TIMESTAMP) AS MaxDate
	 FROM ETL.Date
	 WHERE YearId > 1900 AND YearId < 9999) D

WHERE 1=1
AND P.YearId >= 2005
	AND P.MonthId <= YEAR(current_timestamp() - INTERVAL 1 DAY) * 100 + MONTH(current_timestamp() - INTERVAL 1 DAY)
;
