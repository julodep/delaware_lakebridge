/****** Object:  View [DataStore3].[V_InventoryMovements]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DataStore3`.`V_InventoryMovements` AS


SELECT    RecId
		, TransType
		, InventTransCode
		, InventDimCode
		, I.CompanyCode
		, I.ProductCode
		, WarehouseLocationCode
		, InventLocationCode
		, InventBatchCode
		, SalesInvoiceCode
		, DatePhysical
		, DateFinancial
		, DateClosed

		, InventoryUnit

		, COALESCE(CASE WHEN I.InventoryUnit = P.ProductInventoryUnit THEN I.QTY 
ELSE I.QTY * UOM0.Factor 
END, 0) AS Quantity_InventoryUnit
		, COALESCE(CASE WHEN I.InventoryUnit = P.ProductSalesUnit THEN I.QTY 
ELSE I.QTY * UOM2.Factor 
END, 0) AS Quantity_SalesUnit
		, COALESCE(CASE WHEN I.InventoryUnit = P.ProductPurchaseUnit THEN I.QTY 
ELSE I.QTY * UOM3.Factor 
END, 0) AS Quantity_PurchaseUnit

		, COALESCE(L.AccountingCurrency, '_N/A') AS Currency

		, COALESCE(CostPhysicalTC, 0) AS CostPhysicalTC
		, COALESCE(CostPhysicalTC, 0) AS CostPhysicalAC
		, COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency THEN CostPhysicalTC 
ELSE CostPhysicalTC * RC.ExchangeRate 
END, 0) AS CostPhysicalRC
		, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN CostPhysicalTC 
ELSE CostPhysicalTC * GC.ExchangeRate 
END, 0) AS CostPhysicalGC
		, COALESCE(CostFinancialTC, 0) AS CostFinancialTC
		, COALESCE(CostFinancialTC, 0) AS CostFinancialAC
		, COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency 
THEN CostFinancialTC ELSE CostFinancialTC * RC.ExchangeRate 
END, 0) AS CostFinancialRC
		, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency 
THEN CostFinancialTC ELSE CostFinancialTC * GC.ExchangeRate 
END, 0) AS CostFinancialGC

		, PriceMatch

		, CAST(1 as DECIMAL(38,6)) AS AppliedExchangeRateTC
		, CAST(1 as DECIMAL(38,6)) AS AppliedExchangeRateRC
		, CAST(1 as DECIMAL(38,6)) AS AppliedExchangeRateAC
		, COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
	
FROM DataStore2.InventoryMovements I

--Required for currency conversion
INNER JOIN
	(
	SELECT	DISTINCT LES.AccountingCurrency
			, LES.ReportingCurrency
			, LES.`Name`
			, LES.ExchangeRateType
			, LES.BudgetExchangeRateType
			, GroupCurrency = G.GroupCurrencyCode
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON I.CompanyCode = L.`Name`

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrency
ON RC.FromCurrencyCode = I.CurrencyCode
	and RC.ToCurrencyCode = L.ReportingCurrency
	and RC.ExchangeRateTypeCode = L.ExchangeRateType
	and I.DateFinancial BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = L.AccountingCurrency
	and GC.ToCurrencyCode = L.GroupCurrency
	and GC.ExchangeRateTypeCode = L.ExchangeRateType
	and I.DateFinancial BETWEEN GC.ValidFrom AND GC.ValidTo

LEFT JOIN DataStore.Product P
ON I.ProductCode = P.ProductCode
	and I.CompanyCode = P.CompanyCode

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON I.ProductCode = UOM0.ItemNumber
	and I.CompanyCode = UOM0.CompanyCode 
	and UOM0.FromUOM = I.InventoryUnit 
	and UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON I.ProductCode = UOM2.ItemNumber 
	and I.CompanyCode = UOM2.CompanyCode 
	and UOM2.FromUOM = I.InventoryUnit 
	and UOM2.ToUOM = P.ProductSalesUnit

LEFT JOIN DataStore.UnitOfMeasure UOM3
ON I.ProductCode = UOM3.ItemNumber 
	and I.CompanyCode = UOM3.CompanyCode 
	and UOM3.FromUOM = I.InventoryUnit 
	and UOM3.ToUOM = P.ProductPurchaseUnit
;
