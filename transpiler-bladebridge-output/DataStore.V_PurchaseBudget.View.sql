/****** Object:  View [DataStore].[V_PurchaseBudget]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DataStore`.`V_PurchaseBudget` AS


-- Budget will only be available on monthly basis, so limit the dataset to the following dates
;
WITH DateRange AS (

SELECT	DISTINCT BudgetDate = CAST(LAST_DAY(TIMESTAMP) AS TIMESTAMP)
FROM DataStore.Date D
WHERE 1=1
	and TIMESTAMP >= (SELECT MIN(StartDate) FROM dbo.SMRBIForecastSupplyForecastStaging) --Take earliest budget date
	and TIMESTAMP <= (SELECT MAX(EndDate) FROM dbo.SMRBIForecastSupplyForecastStaging) --Take latest budget date
	and TIMESTAMP > DATEADD(YEAR, CAST(DATEDIFF(0, current_timestamp()) / 365 AS INT) - 2, 0) --Take a maximum of 2 years
	and TIMESTAMP < DATEADD(YEAR, CAST(DATEDIFF(0, current_timestamp()) / 365 AS INT) + 2, 0) --Take a maximum of 2 years
)

SELECT
	--Dimensions
		  COALESCE(NULLIF(UPPER(FSFS.ItemId),''), '_N/A') AS ProductCode
		, COALESCE(NULLIF(FSFS.ModelId,''), '_N/A') AS ForecastModelCode
		, COALESCE(NULLIF(UPPER(FSFS.DataAreaId),''), '_N/A') AS CompanyCode
		, COALESCE(NULLIF(FSFS.VendAccountId, ''), '_N/A') AS SupplierCode
		, COALESCE(NULLIF(L.ExchangeRateType,''), '_N/A') AS DefaultExchangeRateTypeCode
		, COALESCE(NULLIF(L.BudgetExchangeRateType,''), '_N/A') AS BudgetExchangeRateTypeCode
		, COALESCE(NULLIF(FSFS.Currency,''), '_N/A') AS TransactionCurrencyCode
		, COALESCE(NULLIF(L.AccountingCurrency,''), '_N/A') AS AccountingCurrencyCode
		, COALESCE(NULLIF(L.ReportingCurrency,''), '_N/A') AS ReportingCurrencyCode
		, COALESCE(NULLIF(L.GroupCurrency,''), '_N/A') AS GroupCurrencyCode

	--Technical Fields
		, COALESCE(FSFS.FORECASTSUPPLYFORECASTDIMENSION, -1) AS DefaultDimension 
		, COALESCE(FSFS.InventDimId, '_N/A') AS InventDimCode

	--Dates 
		, DR.BudgetDate AS BudgetDate

	--Measures	
		, COALESCE(NULLIF(FSFS.PurchUnitId,''), '_N/A') AS PurchaseUnit 
		, COALESCE(FSFS.PurchQTY, 0) AS BudgetQuantity
		, COALESCE(FSFS.PurchPrice, 0) AS PurchUnitPriceTC 
		, COALESCE(CASE WHEN FSFS.Currency = L.AccountingCurrency 
THEN FSFS.PurchPrice 
ELSE FSFS.PurchPrice * AC.ExchangeRate 
END, 0) AS PurchUnitPriceAC
		, COALESCE(CASE WHEN FSFS.Currency = L.ReportingCurrency 
THEN FSFS.PurchPrice 
ELSE FSFS.PurchPrice * RC.ExchangeRate 
END, 0) AS PurchUnitPriceRC
		, COALESCE(CASE WHEN FSFS.Currency = L.GroupCurrency 
THEN FSFS.PurchPrice 
ELSE FSFS.PurchPrice * GC.ExchangeRate 
END, 0) AS PurchUnitPriceGC
		, COALESCE(CASE WHEN FSFS.Currency = L.AccountingCurrency 
THEN FSFS.PurchPrice 
ELSE FSFS.PurchPrice * AC_Budget.ExchangeRate 
END, 0) AS PurchUnitPriceAC_Budget 
		, COALESCE(CASE WHEN FSFS.Currency = L.ReportingCurrency 
THEN FSFS.PurchPrice 
ELSE FSFS.PurchPrice * RC_Budget.ExchangeRate 
END, 0) AS PurchUnitPriceRC_Budget 
		, COALESCE(CASE WHEN FSFS.Currency = L.GroupCurrency 
THEN FSFS.PurchPrice 
ELSE FSFS.PurchPrice * GC_Budget.ExchangeRate 
END, 0) AS PurchUnitPriceGC_Budget
		, COALESCE(FSFS.Amount, 0) AS BudgetAmountTC 
		, COALESCE(CASE WHEN FSFS.Currency = L.AccountingCurrency 
THEN FSFS.Amount 
ELSE FSFS.Amount * AC.ExchangeRate 
END, 0) AS BudgetAmountAC
		, COALESCE(CASE WHEN FSFS.Currency = L.ReportingCurrency 
THEN FSFS.Amount 
ELSE FSFS.Amount * RC.ExchangeRate 
END, 0) AS BudgetAmountRC 
		, COALESCE(CASE WHEN FSFS.Currency = L.GroupCurrency 
THEN FSFS.Amount 
ELSE FSFS.Amount * GC.ExchangeRate 
END, 0) AS BudgetAmountGC
		, COALESCE(CASE WHEN FSFS.Currency = L.AccountingCurrency 
THEN FSFS.Amount 
ELSE FSFS.Amount * AC_Budget.ExchangeRate 
END, 0) AS BudgetAmountAC_Budget
		, COALESCE(CASE WHEN FSFS.Currency = L.ReportingCurrency 
THEN FSFS.Amount 
ELSE FSFS.Amount * RC_Budget.ExchangeRate 
END, 0) AS BudgetAmountRC_Budget 
		, COALESCE(CASE WHEN FSFS.Currency = L.GroupCurrency 
THEN FSFS.Amount 
ELSE FSFS.Amount * GC_Budget.ExchangeRate 
END, 0) AS BudgetAmountGC_Budget 
		, CAST(1 as DECIMAL(38,6)) AS AppliedExchangeRateTC 
		, COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC 
		, COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
		, COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
		, COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
		, COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
		, COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget

FROM DateRange DR

INNER JOIN dbo.SMRBIForecastSupplyForecastStaging FSFS
ON DR.BudgetDate >= FSFS.StartDate
	and DR.BudgetDate <= FSFS.EndDate

-- Required for Currencies:
INNER JOIN 
	(SELECT DISTINCT LES.ReportingCurrency
			, LES.AccountingCurrency
			, LES.ExchangeRateType
			, LES.BudgetExchangeRateType
			, LES.`Name`
			, GroupCurrency = G.GroupCurrencyCode
	 FROM dbo.SMRBILedgerStaging LES
	 CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON FSFS.DataAreaId = L.Name

-- Required for the Actual exchange rates:

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrenycy
ON RC.FromCurrencyCode = FSFS.Currency
	and RC.ToCurrencyCode = L.ReportingCurrency
	and RC.ExchangeRateTypeCode = L.ExchangeRateType
	and FSFS.StartDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = FSFS.Currency
	and AC.ToCurrencyCode = L.AccountingCurrency
	and AC.ExchangeRateTypeCode = L.ExchangeRateType
	and FSFS.StartDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = FSFS.Currency
	and GC.ToCurrencyCode = L.GroupCurrency
	and GC.ExchangeRateTypeCode = L.ExchangeRateType
	and FSFS.StartDate BETWEEN GC.ValidFrom AND GC.ValidTo

-- Required for the Budget exchange rates:

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrenycy
ON RC_Budget.FromCurrencyCode = FSFS.Currency
	and RC_Budget.ToCurrencyCode = L.ReportingCurrency
	and RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
	and FSFS.StartDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = FSFS.Currency
	and AC_Budget.ToCurrencyCode = L.AccountingCurrency
	and AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
	and FSFS.StartDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = FSFS.Currency  
	and GC_Budget.ToCurrencyCode = L.GroupCurrency
	and GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
	and FSFS.StartDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo
;
