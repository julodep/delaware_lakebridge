/****** Object:  View [DataStore2].[V_InterCompanyARAP]    Script Date: 03/03/2026 16:26:09 ******/














/****** Script for SelectTopNRows command from SSMS  ******/


CREATE OR REPLACE VIEW `DataStore2`.`V_InterCompanyARAP` AS 

WITH AP AS (
	SELECT    ReportDate
			, CompanyCode
			, YEAR(InvoiceDate) AS YearInvoice
			, PurchaseInvoiceCode
			, SUM(OpenAmountTC) AS OpenAmountTC
			, SUM(OpenAmountAC) AS OpenAmountAC
			, SUM(OpenAmountRC) AS OpenAmountRC
			, SUM(OpenAmountGC) AS OpenAmountGC
			, SUM(InvoiceAmountAC) AS InvoiceAmountAC
			, SUM(InvoiceAmountGC) AS InvoiceAmountGC
			, SUM(InvoiceAmountTC) AS InvoiceAmountTC
			, SUM(InvoiceAmountRC) AS InvoiceAmountRC
	FROM DataStore.AccountsPayable 
	WHERE PurchaseInvoiceCode <> '_N/A'
	GROUP BY  ReportDate
			, CompanyCode
			, YEAR(InvoiceDate) 
			, PurchaseInvoiceCode
	)

,  AR AS (
	SELECT    ReportDate
			, CompanyCode
			, YEAR(InvoiceDate) AS YearInvoice
			, SalesInvoiceCode
			, SUM(OpenAmountTC) AS OpenAmountTC
			, SUM(OpenAmountAC) AS OpenAmountAC
			, SUM(OpenAmountRC) AS OpenAmountRC
			, SUM(OpenAmountGC) AS OpenAmountGC
			, SUM(InvoiceAmountAC) AS InvoiceAmountAC
			, SUM(InvoiceAmountGC) AS InvoiceAmountGC
			, SUM(InvoiceAmountTC) AS InvoiceAmountTC
			, SUM(InvoiceAmountRC) AS InvoiceAmountRC
	FROM DataStore.AccountsReceivable
	WHERE SalesInvoiceCode <> '_N/A'
	GROUP BY  ReportDate
			, CompanyCode
			, YEAR(InvoiceDate) 
			, SalesInvoiceCode
	)

SELECT 
 
	  ARAP.SalesInvoiceCode
	, ARAP.PurchaseInvoiceCode
	, COALESCE(AR.ReportDate, AP.ReportDate, 19000101) AS ReportDate
	, SupplierCode
	, CustomerCode
	, L.ExchangeRateType AS DefaultExchangeRateTypeCode	
	, Currency AS TransactionCurrencyCode	
	, COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), '_N/A') AS AccountingCurrencyCode
	, COALESCE(CAST(UPPER(L.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
	, COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode
	, InvoiceTotal AS InvoiceAmountTC
	, COALESCE(CASE WHEN ARAP.Currency = L.AccountingCurrency THEN (InvoiceTotal) 
ELSE (InvoiceTotal) * AC.ExchangeRate END, 0) AS InvoiceAmountAC
	 , COALESCE(CASE WHEN ARAP.Currency = L.ReportingCurrency THEN (InvoiceTotal) 
ELSE (InvoiceTotal) * RC.ExchangeRate END, 0) AS InvoiceAmountRC

	 , COALESCE(CASE WHEN ARAP.Currency = L.GroupCurrency THEN (InvoiceTotal) 
ELSE (InvoiceTotal) * GC.ExchangeRate END, 0) AS InvoiceAmountGC
	, Coalesce(AR.OpenAmountTC, AP.OpenAmountTC) AS OpenAmountTC
	, Coalesce(AR.OpenAmountAC,AP.OpenAmountAC) AS  OpenAmountAC
	, Coalesce(AR.OpenAmountRC,AP.OpenAmountRC) AS OpenAmountRC
	, Coalesce(AR.OpenAmountGC,AP.OpenAmountGC) AS OpenAmountGC
	, Coalesce(AR.InvoiceAmountTC, AP.InvoiceAmountTC) AS AmountInvoiceTC
	, Coalesce(AR.InvoiceAmountAC, AP.InvoiceAmountAC) AS AmountInvoiceAC
	, Coalesce(AR.InvoiceAmountRC, AP.InvoiceAmountRC) AS AmountInvoiceRC
	, Coalesce(AR.InvoiceAmountGC, AP.InvoiceAmountGC) AS AmountInvoiceGC
	, CASE WHEN AR_AP_Type = 'AP' THEN AP_Settlement ELSE '_N/A' END AS SupplierSettlement
	, CASE WHEN AR_AP_Type = 'AR' THEN AR_Settlement ELSE '_N/A' END AS CustomerSettlement
	, CAST(date_format(InvoiceDate, 'yyyyMMdd') AS INT) AS DimInvoiceDateId
	, CAST(date_format(DueDate, 'yyyyMMdd') AS INT) AS DimDueDateId
	, PostedDate
	, ETA
	, ETD
	, Brn AS Branch
	, Departement
	, CASE WHEN RIGHT(Departement, 1) = 'S'
	       THEN 'OFF'
		   WHEN RIGHT(Departement, 1) = 'A'
		   THEN 'AFF'
		   ELSE 'OTH'
	  END AS BusinessType
	, AR_AP_Type
	, Type	
	, JobInvoice	
	, DestDisch AS POD
	, OriginLoad AS POL
	, House
	, JobNumber AS ShipmentNumber
	, Master
	, OrigCountry
	, OrigCountryName	
	, CAST(ARAP.CompanyCode AS STRING) AS CompanyCode
  FROM DataStore.InterCompanyARAP ARAP
	LEFT JOIN AP AP
		ON ARAP.PurchaseInvoiceCode = AP.PurchaseInvoiceCode
		AND ARAP.CompanyCode = AP.CompanyCode
		AND ARAP.YearInvoice = AP.YearInvoice
		AND ARAP.AR_AP_Type = 'AP'
	LEFT JOIN AR AR
		ON ARAP.SalesInvoiceCode = AR.SalesInvoiceCode
		AND ARAP.CompanyCode = AR.CompanyCode 
		AND ARAP.YearInvoice = AR.YearInvoice
		AND ARAP.AR_AP_Type = 'AR'
	INNER JOIN
	(
	SELECT	DISTINCT LES.AccountingCurrency
			       , LES.ReportingCurrency
			       , LES.`Name`
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , G.GroupCurrencyCode AS GroupCurrency
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON ARAP.CompanyCode = L.`Name`

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurreny
ON RC.FromCurrencyCode = ARAP.Currency
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND ARAP.InvoiceDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = ARAP.Currency
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND ARAP.InvoiceDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = ARAP.Currency 
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND ARAP.InvoiceDate BETWEEN GC.ValidFrom AND GC.ValidTo

UNION ALL

	SELECT    '_N/A' AS SalesInvoiceCode
			, AP.PurchaseInvoiceCode
			, AP.ReportDate
			, AP.SupplierCode
			, '_N/A' AS CustomerCode
			, L.EXCHANGERATETYPE AS DefaultExchangeRateType
			, AP.TransactionCurrencyCode
			, AP.AccountingCurrencyCode
			, AP.ReportingCurrencyCode
			, AP.GroupCurrencyCode
			, NULL AS InvoiceAmountAC
			, NULL AS InvoiceAmountGC
			, NULL AS InvoiceAmountTC
			, NULL AS InvoiceAmountRC
			, SUM(OpenAmountTC) AS OpenAmountTC
			, SUM(OpenAmountAC) AS OpenAmountAC
			, SUM(OpenAmountRC) AS OpenAmountRC
			, SUM(OpenAmountGC) AS OpenAmountGC
			, SUM(InvoiceAmountAC) AS AmountInvoice
			, SUM(InvoiceAmountGC) AS AmountInvoiceGC
			, SUM(InvoiceAmountTC) AS AmountInvoiceTC
			, SUM(InvoiceAmountRC) AS AmountInvoiceRC
			, AP.SupplierCode AS SupplierSettlement
			, '_N/A' AS CustomerSettlement
			, CAST(date_format(AP.InvoiceDate, 'yyyyMMdd') AS INT) AS DimInvoiceDateId
			, CAST(date_format(AP.DueDate, 'yyyyMMdd') AS INT) AS DimDueDateId
			, '1900-01-01' AS PostedDate
			, '1900-01-01' AS ETA
			, '1900-01-01' AS ETD
			, '_N/A' AS Branch
			, '_N/A' AS Department
			, 'OTH' AS BusinessType
			, 'AP' AS AR_AP_Type
			, '_N/A' AS Type
			, '_N/A' AS JobInvoice
			, '_N/A' AS POD
			, '_N/A' AS POL
			, '_N/A' AS House
			, '_N/A' AS ShipmentNumber
			, '_N/A' AS Master
			, '_N/A' AS OrigCountry
			, '_N/A' AS OrigCountryName
			, AP.CompanyCode

	FROM DataStore.AccountsPayable AP

		INNER JOIN
	(
	SELECT	DISTINCT LES.AccountingCurrency
			       , LES.ReportingCurrency
			       , LES.`Name`
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , G.GroupCurrencyCode AS GroupCurrency
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON AP.CompanyCode = L.`Name`

LEFT JOIN DataStore.IntercompanyARAP ARAP
ON AP.CompanyCode = ARAP.CompanyCode
AND AP.PurchaseInvoiceCode = ARAP.PurchaseInvoiceCode
AND ARAP.YearInvoice = YEAR(AP.InvoiceDate)
AND ARAP.AR_AP_Type = 'AP'

WHERE AP.PurchaseInvoiceCode <> '_N/A'
AND ARAP.PurchaseInvoiceCode IS NULL
AND AP.InvoiceAmountTC <> 0

GROUP BY  AP.PurchaseInvoiceCode
			, AP.ReportDate
			, AP.SupplierCode
			, L.EXCHANGERATETYPE
			, AP.TransactionCurrencyCode
			, AP.AccountingCurrencyCode
			, AP.ReportingCurrencyCode
			, AP.GroupCurrencyCode
			, AP.InvoiceDate
			, AP.DueDate
			, AP.CompanyCode

UNION ALL


	SELECT    AR.SalesInvoiceCode AS SalesInvoiceCode
			, '_N/A' AS PurchaseInvoiceCode
			, AR.ReportDate
			, '_N/A' AS SupplierCode
			, AR.CustomerCode AS CustomerCode
			, L.EXCHANGERATETYPE AS DefaultExchangeRateType
			, AR.TransactionCurrencyCode
			, AR.AccountingCurrencyCode
			, AR.ReportingCurrencyCode
			, AR.GroupCurrencyCode
			, NULL AS InvoiceAmountAC
			, NULL AS InvoiceAmountGC
			, NULL AS InvoiceAmountTC
			, NULL AS InvoiceAmountRC
			, SUM(OpenAmountTC) AS OpenAmountTC
			, SUM(OpenAmountAC) AS OpenAmountAC
			, SUM(OpenAmountRC) AS OpenAmountRC
			, SUM(OpenAmountGC) AS OpenAmountGC
			, SUM(InvoiceAmountAC) AS AmountInvoice
			, SUM(InvoiceAmountGC) AS AmountInvoiceGC
			, SUM(InvoiceAmountTC) AS AmountInvoiceTC
			, SUM(InvoiceAmountRC) AS AmountInvoiceRC
			, '_N/A' AS SupplierSettlement
			, AR.CustomerCode AS CustomerSettlement
			, CAST(date_format(AR.InvoiceDate, 'yyyyMMdd') AS INT) AS DimInvoiceDateId
			, CAST(date_format(AR.DueDate, 'yyyyMMdd') AS INT) AS DimDueDateId
			, '1900-01-01' AS PostedDate
			, '1900-01-01' AS ETA
			, '1900-01-01' AS ETD
			, '_N/A' AS Branch
			, '_N/A' AS Department
			, 'OTH' AS BusinessType
			, 'AR' AS AR_AP_Type
			, '_N/A' AS Type
			, '_N/A' AS JobInvoice
			, '_N/A' AS POD
			, '_N/A' AS POL
			, '_N/A' AS House
			, '_N/A' AS ShipmentNumber
			, '_N/A' AS Master
			, '_N/A' AS OrigCountry
			, '_N/A' AS OrigCountryName
			, AR.CompanyCode

	FROM DataStore.AccountsReceivable AR

		INNER JOIN
	(
	SELECT	DISTINCT LES.AccountingCurrency
			       , LES.ReportingCurrency
			       , LES.`Name`
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , G.GroupCurrencyCode AS GroupCurrency
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON AR.CompanyCode = L.`Name`

LEFT JOIN DataStore.IntercompanyARAP ARAP
ON AR.CompanyCode = ARAP.CompanyCode
AND AR.SalesInvoiceCode = ARAP.SalesInvoiceCode
AND ARAP.YearInvoice = YEAR(AR.InvoiceDate)
AND ARAP.AR_AP_Type = 'AR'

WHERE ARAP.SalesInvoiceCode IS NULL
AND AR.InvoiceAmountTC <> 0

GROUP BY  AR.SalesInvoiceCode
			, AR.ReportDate
			, AR.CustomerCode
			, L.EXCHANGERATETYPE
			, AR.TransactionCurrencyCode
			, AR.AccountingCurrencyCode
			, AR.ReportingCurrencyCode
			, AR.GroupCurrencyCode
			, AR.InvoiceDate
			, AR.DueDate
			, AR.CompanyCode

	
	
		
;
