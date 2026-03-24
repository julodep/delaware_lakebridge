/****** Object:  View [DWH].[V_DPO1]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DPO1` AS 

--Purpose: determine the total balance of purchase invoices (FactPurchaseInvoice) and the open amount (FactAccountsPayable) during a particular month, for a particular supplier

--Count the number of (working) days in a certain month
;
WITH CalendarDays AS (
	SELECT	DISTINCT D1.MonthId
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

--Take the sum of the gross purchase, per month, per supplier
--Check! The purchase invoices do not include the VAT (GST) amount, whereas the AR balance does. As such, include the VAT amount in the Turnover amount
Turnover AS (
	
	SELECT	DimSupplierId
		  , CompanyCode
		  , MonthId
		  , SUM(Turnover) AS Turnover
	FROM
		(SELECT DS.DimSupplierId
			  , DS.CompanyCode
			  , COALESCE(DD.MonthId, CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS INT)) AS MonthId
			  , ROUND((SUM(COALESCE(FPI.GrossPurchaseGC, 0))
				- SUM(COALESCE(FPI.DiscountAmountGC, 0))) 
				* (1 + CAST(COALESCE(NULLIF(LTRIM(RTRIM(REPLACE(FPI.TaxWriteCode,'%',''))), ''), 0) AS DECIMAL(38,6)) / 100), 2) AS Turnover
		FROM DWH.DimSupplier DS
		LEFT JOIN DWH.FactPurchaseInvoice FPI --WHERE DimSupplierId = 14 --> Temporary code: REMOVE !!!
		ON DS.DimSupplierId = FPI.DimSupplierId
		LEFT JOIN DWH.DimDate DD
		ON FPI.DimInvoiceDateId = DD.DimDateId
		WHERE 1=1
		--AND DS.DimSupplierId = 3401 --> Temporary code: REMOVE !!!
		GROUP BY DS.DimSupplierId
			   , DD.MonthId
			   , DS.CompanyCode
			   , FPI.TaxWriteCode) T
	GROUP BY DimSupplierId, CompanyCode, MonthId	
	--ORDER BY DimSupplierId, MonthId --> Temporary code: REMOVE !!!
),

--Take the sum of the open amount, per supplier, current situation
OpenAmount AS (
	SELECT DS.DimSupplierId
		 , COALESCE(DD.MonthId, CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS INT)) AS MonthId
		 , SUM(COALESCE(FAP.OpenAmountGC, 0)) AS OpenAmount
		 , DS.CompanyCode
	FROM DWH.DimSupplier DS
	LEFT JOIN DWH.FactAccountsPayable FAP --FactAccountsPayable WHERE DimSupplierId = 14 --> Temporary code: REMOVE !!!
	ON DS.DimSupplierId = FAP.DimSupplierId
	LEFT JOIN DWH.DimDate DD
	ON FAP.DimReportDateId = DD.DimDateId
	WHERE 1=1
	--AND DS.DimSupplierId = 3401 --> Temporary code: REMOVE !!!
	GROUP BY DS.DimSupplierId, DD.MonthId, DS.CompanyCode
	--ORDER BY DD.MonthId --> Temporary code: REMOVE !!!
)

SELECT DS.DimSupplierId
	 , C.DimCompanyId
	 , DD.MonthId
	 , WD.CalendarDays
	 --, WD.WorkingDays --Currently excluded, add if required!
	 , COALESCE(T.Turnover, 0) AS Turnover
	 , COALESCE(OA.OpenAmount, 0) AS OpenAmount
FROM DWH.DimSupplier DS
CROSS JOIN (SELECT DISTINCT MonthId FROM CalendarDays) DD --Limit to active months
LEFT JOIN Turnover T
ON T.DimSupplierId = DS.DimSupplierId
AND T.MonthId = DD.MonthId
AND T.CompanyCode = DS.CompanyCode
LEFT JOIN CalendarDays WD
ON WD.MonthId = DD.MonthId
LEFT JOIN OpenAmount OA
ON DS.DimSupplierId = OA.DimSupplierId 
AND OA.MonthId = DD.MonthId
AND DS.CompanyCode = OA.CompanyCode
LEFT JOIN DWH.DimCompany C
ON DS.CompanyCode = C.CompanyCode
WHERE 1=1
AND DD.MonthId != 190001 
AND DD.MonthId < CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS INT) --Do not include the current month
AND DS.SupplierGroupCode != 'IC' --Intercompany suppliers are not taken into account for DPO calculation

--AND DS.DimSupplierId IN (3401) --> Temporary code: REMOVE !!!

--ORDER BY DS.DimSupplierId, DD.MonthId --> Temporary code: REMOVE !!!
;
