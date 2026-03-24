/****** Object:  View [DataStore].[V_Inventory]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DataStore`.`V_Inventory` AS



SELECT
	/*Dimensions*/
	  COALESCE(NULLIF(UPPER(InventSum.ItemId),''), '_N/A')	AS ProductCode
	, COALESCE(NULLIF(UPPER(InventSum.Company),''), '_N/A') AS CompanyCode
	, COALESCE(NULLIF(InventSum.InventDimId,''), '_N/A') AS ProductConfigurationCode
	, COALESCE(NULLIF(ProductConfiguration.InventBatchCode,''), '_N/A') AS BatchCode
	--, IsCurrentDate = N'Yes'
	--Report Date: Cater for multiple runs a day
	--At the moment, this will be the PREVIOUS DAY STOCK
	, CAST(CAST(current_timestamp() AS DATE) AS TIMESTAMP) AS ReportDate
	, COALESCE(NULLIF(L.ExchangeRateType,''), '_N/A') AS DefaultExchangeRateTypeCode
	, COALESCE(NULLIF(L.BudgetExchangeRateType,''), '_N/A') AS BudgetExchangeRateTypeCode
	, COALESCE(NULLIF(L.AccountingCurrency,''), '_N/A') AS AccountingCurrencyCode
	, COALESCE(NULLIF(L.ReportingCurrency,''), '_N/A') AS ReportingCurrencyCode
	, COALESCE(NULLIF(L.GroupCurrency,''), '_N/A') AS GroupCurrencyCode
	/*Inventory Details*/
	, COALESCE(NULLIF(InventTableModule.UnitId,''), '_N/A') AS InventoryUnit
	, COALESCE(InventSum.PhysicalInvent, 0) AS StockQuantity
	, COALESCE(CASE WHEN L.AccountingCurrency = L.AccountingCurrency THEN InventSum.PhysicalInvent * CP.Price ELSE InventSum.PhysicalInvent * AC.ExchangeRate END, 0) AS StockValueAC
	, COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency THEN InventSum.PhysicalInvent * CP.Price ELSE InventSum.PhysicalInvent * RC.ExchangeRate END, 0) AS StockValueRC
	, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN InventSum.PhysicalInvent * CP.Price ELSE InventSum.PhysicalInvent * GC.ExchangeRate END, 0) AS StockValueGC
	, COALESCE(CASE WHEN L.AccountingCurrency = L.AccountingCurrency THEN InventSum.PhysicalInvent * CP.Price ELSE InventSum.PhysicalInvent * AC_Budget.ExchangeRate END, 0) AS StockValueAC_Budget
	, COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency THEN InventSum.PhysicalInvent * CP.Price ELSE InventSum.PhysicalInvent * RC_Budget.ExchangeRate END, 0) AS StockValueRC_Budget
	, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN InventSum.PhysicalInvent * CP.Price ELSE InventSum.PhysicalInvent * GC_Budget.ExchangeRate END, 0) AS StockValueGC_Budget
	, COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
	, COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
	, COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
	, COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
	, COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
	, COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget

FROM dbo.SMRBIInventSumStaging AS InventSum

LEFT JOIN dbo.SMRBIInventTableModuleStaging AS InventTableModule
ON  InventTableModule.ItemId = InventSum.ItemId
AND InventTableModule.Company = InventSum.Company
AND InventTableModule.ModuleType = '0'

LEFT JOIN DataStore.ProductConfiguration AS ProductConfiguration
ON  ProductConfiguration.CompanyCode = InventSum.Company
AND ProductConfiguration.InventDimCode = InventSum.InventDimId

LEFT JOIN 
	(SELECT DISTINCT ItemNumber
			       , UnitCode
			       , CP.CompanyCode
			       , ProductConfigurationCode
			       , Price
			       , StartValidityDate
			       , EndValidityDate
		FROM DataStore.CostPrice CP
		JOIN DataStore.ProductConfiguration PC
		ON CP.CompanyCode = PC.CompanyCode
		AND CP.InventDimCode = PC.InventDimCode) CP --Include ProductConfiguration in the join
ON InventSum.ItemId = CP.ItemNumber
AND UPPER(InventSum.Company) = CP.CompanyCode
AND InventTableModule.UnitId = CP.UnitCode
AND CAST(CAST(current_timestamp() AS DATE) AS TIMESTAMP) >= CP.StartValidityDate
AND CAST(CAST(current_timestamp() AS DATE) AS TIMESTAMP) <= CP.EndValidityDate
AND CP.ProductConfigurationCode = ProductConfiguration.ProductConfigurationCode

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
ON InventSum.Company = L.Name

--Required for the Actual exchange rates:
LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrenycy
ON RC.FromCurrencyCode = L.AccountingCurrency
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND current_timestamp() BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = L.AccountingCurrency
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND current_timestamp() BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = L.AccountingCurrency
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND current_timestamp() BETWEEN GC.ValidFrom AND GC.ValidTo

--Required for the Budget exchange rates:

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrenycy
ON RC_Budget.FromCurrencyCode = L.AccountingCurrency
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND current_timestamp() BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = L.AccountingCurrency
AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND current_timestamp() BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = L.AccountingCurrency  
AND GC_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND current_timestamp() BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo
;
