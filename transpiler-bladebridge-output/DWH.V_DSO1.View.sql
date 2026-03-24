/****** Object:  View [DWH].[V_DSO1]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DSO1` AS 

--Purpose: determine the total balance of sales invoices (FactSalesInvoice) and the open amount (FactAccountsReceivable) during a particular month, for a particular customer

--Count the number of (working) days in a certain month
;
WITH CalendarDays AS (
	SELECT DISTINCT D1.MonthId
				  , COUNT(D2.DimDateId) AS WorkingDays
				  , COUNT(D1.DimDateId) AS CalendarDays
	FROM DWH.DimDate D1
	LEFT JOIN DWH.DimDate D2
	ON D1.DimDateId = D2.DimDateId
	AND D2.DayOfWeekName NOT IN ('Saturday', 'Sunday')
	
	WHERE 1=1
	AND D1.YearId >= (SELECT YearId - 2 FROM DWH.DimDate WHERE TIMESTAMP IN (SELECT CAST(CAST(current_timestamp() AS DATE) AS TIMESTAMP))) --Go back MAX 2 years
	AND D1.MonthId < (SELECT MonthId FROM DWH.DimDate WHERE TIMESTAMP = CAST(CAST(current_timestamp() AS DATE) AS TIMESTAMP)) --Exclude current month

	GROUP BY D1.MonthId
),

--Take the sum of the gross sales, per month, per customer
--Check! The sales invoices do not include the VAT (GST) amount, whereas the AR balance does. As such, include the VAT amount in the Turnover amount
Turnover AS (	
	SELECT DimCustomerId
		 , CompanyCode
		 , MonthId
		 , SUM(Turnover) AS Turnover
	FROM
		(SELECT DC.DimCustomerId
			  , DC.CompanyCode
			  , COALESCE(DD.MonthId, CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS INT)) AS MonthId
			  , ROUND((SUM(COALESCE(FSI.GrossSalesGC, 0))
			  	- SUM(COALESCE(FSI.DiscountAmountGC, 0))) 
				* (1 + CAST(COALESCE(NULLIF(LTRIM(RTRIM(REPLACE(FSI.TaxWriteCode,'%',''))), ''), 0) AS DECIMAL(38,6)) / 100), 2) AS Turnover
		FROM DWH.DimCustomer DC
		LEFT JOIN DWH.FactSalesInvoice FSI --WHERE DimCustomerId = 14 --> Temporary code: REMOVE !!!
		ON DC.DimCustomerId = FSI.DimCustomerId
		LEFT JOIN DWH.DimDate DD
		ON FSI.DimInvoiceDateId = DD.DimDateId
		WHERE 1=1
		--AND DC.DimCustomerId = 3401 --> Temporary code: REMOVE !!!
		GROUP BY DC.DimCustomerId
			   , DD.MonthId
			   , DC.CompanyCode
			   , FSI.TaxWriteCode) T
	GROUP BY DimCustomerId, CompanyCode, MonthId
	
	--ORDER BY DimCustomerId, MonthId --> Temporary code: REMOVE !!!
),

--Take the sum of the open amount, per customer, current situation
OpenAmount AS (
	SELECT DC.DimCustomerId
		 , COALESCE(DD.MonthId, CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS INT)) AS MonthId
		 , SUM(COALESCE(FAR.OpenAmountGC, 0)) AS OpenAmount
		 , DC.CompanyCode
	FROM DWH.DimCustomer DC
	LEFT JOIN DWH.FactAccountsReceivable FAR --FactAccountsReceivable WHERE DimCustomerId = 14 --> Temporary code: REMOVE !!!
	ON DC.DimCustomerId = FAR.DimCustomerId
	LEFT JOIN DWH.DimDate DD
	ON FAR.DimReportDateId = DD.DimDateId
	WHERE 1=1
		--and DC.DimCustomerId = 3401 --> Temporary code: REMOVE !!!
	GROUP BY DC.DimCustomerId, DD.MonthId, DC.CompanyCode
	--ORDER BY DD.MonthId --> Temporary code: REMOVE !!!
)

SELECT	DC.DimCustomerId
	  , C.DimCompanyId
	  , DD.MonthId
	  , WD.CalendarDays
	  --, WD.WorkingDays --Currently excluded, add if required!
	  , COALESCE(T.Turnover, 0) AS Turnover
	  , COALESCE(OA.OpenAmount, 0) AS OpenAmount
FROM DWH.DimCustomer DC
CROSS JOIN (SELECT DISTINCT MonthId FROM CalendarDays) DD --Limit to active months
LEFT JOIN Turnover T
ON T.DimCustomerId = DC.DimCustomerId
AND T.MonthId = DD.MonthId
AND T.CompanyCode = DC.CompanyCode
LEFT JOIN CalendarDays WD
ON WD.MonthId = DD.MonthId
LEFT JOIN OpenAmount OA
ON DC.DimCustomerId = OA.DimCustomerId 
AND OA.MonthId = DD.MonthId
AND DC.CompanyCode = OA.CompanyCode
LEFT JOIN DWH.DimCompany C
ON DC.CompanyCode = C.CompanyCode
WHERE 1=1
AND DD.MonthId != 190001 
AND DD.MonthId < CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS INT) --Do not include the current month
AND DC.CustomerGroup != 'ICO' --Intercompany customers are not taken into account for DSO calculation

--AND DC.DimCustomerId IN (3401) --> Temporary code: REMOVE !!!

--ORDER BY DC.DimCustomerId, DD.MonthId --> Temporary code: REMOVE !!!
;
