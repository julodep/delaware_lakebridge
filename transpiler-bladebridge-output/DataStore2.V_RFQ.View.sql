/****** Object:  View [DataStore2].[V_RFQ]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DataStore2`.`V_RFQ` AS


SELECT CONCAT(RFQ.RFQCaseCode,RFQ.RFQCaseLineNumber,RFQ.RFQCode,RFQ.RFQLineNumber,RFQ.CompanyCode) AS RFQIdScreening
		, RFQ.RFQCaseCode AS RFQCaseCode
		, RFQ.RFQCaseName AS RFQCaseName
		, RFQ.RFQCaseLineNumber AS RFQCaseLineNumber
		, RFQ.RFQCode AS RFQCode
		, RFQ.RFQName AS RFQName
		, RFQ.RFQLineNumber AS RFQLineNumber
	
	/* Dimensions */
		, RFQ.SupplierCode AS SupplierCode
		, RFQ.CompanyCode AS CompanyCode
		, RFQ.ProductConfigurationCode AS ProductConfigurationCode
		, RFQ.ProductCode AS ProductCode
		, RFQ.DeliveryModeCode AS DeliveryModeCode
		, RFQ.DeliveryTermsCode AS DeliveryTermsCode
		, RFQ.DefaultExchangeRateTypeCode AS DefaultExchangeRateTypeCode
		, RFQ.BudgetExchangeRateTypeCode AS BudgetExchangeRateTypeCode
		, CAST(RFQ.TransactionCurrencyCode AS STRING) AS TransactionCurrencyCode
		, RFQ.AccountingCurrencyCode AS AccountingCurrencyCode
		, RFQ.ReportingCurrencyCode AS ReportingCurrencyCode
		, RFQ.GroupCurrencyCode AS GroupCurrencyCode
	
	/* Dates */
		, RFQ.DeliveryDate AS DeliveryDate
		, RFQ.ExpiryDate AS ExpiryDate
		, RFQ.CreatedDate AS CreatedDate
	
	/* RFQ Details */
		, RFQ.RFQCaseStatusLow AS RFQCaseStatusLow
		, RFQ.RFQCaseStatusHigh AS RFQCaseStatusHigh
		, RFQ.RFQLineStatus AS RFQLineStatus
	
	/* Key Figures */
		, RFQ.PurchaseUnit AS PurchUnit

		, COALESCE(CASE WHEN RFQ.PurchaseUnit = P.ProductInventoryUnit 
THEN RFQ.PurchQuantity
ELSE RFQ.PurchQuantity * UOM0.Factor 
END, 0) AS OrderedQuantity_InventoryUnit
		, COALESCE(CASE WHEN RFQ.PurchaseUnit = P.ProductPurchaseUnit 
THEN RFQ.PurchQuantity
ELSE RFQ.PurchQuantity * UOM1.Factor 
END, 0) AS OrderedQuantity_PurchaseUnit
		, COALESCE(CASE WHEN RFQ.PurchaseUnit = P.ProductSalesUnit 
THEN RFQ.PurchQuantity
ELSE RFQ.PurchQuantity * UOM2.Factor 
END, 0) AS OrderedQuantity_SalesUnit

		, PurchPriceTC
		, PurchPriceAC
		, PurchPriceRC
		, PurchPriceGC
		, PurchPriceAC_Budget
		, PurchPriceRC_Budget
		, PurchPriceGC_Budget
		, RFQ.RFQLineAmountTC AS RFQLineAmountTC
		, RFQ.RFQLineAmountAC AS RFQLineAmountAC 
		, RFQ.RFQLineAmountRC AS RFQLineAmountRC 
		, RFQ.RFQLineAmountGC AS RFQLineAmountGC 
		, RFQ.RFQLineAmountAC_Budget AS RFQLineAmountAC_Budget
		, RFQ.RFQLineAmountRC_Budget AS RFQLineAmountRC_Budget
		, RFQ.RFQLineAmountGC_Budget AS RFQLineAmountGC_Budget
		, RFQ.CostAvoidanceAmountTC AS CostAvoidanceAmountTC
		, RFQ.CostAvoidanceAmountAC AS CostAvoidanceAmountAC
		, RFQ.CostAvoidanceAmountRC AS CostAvoidanceAmountRC
		, RFQ.CostAvoidanceAmountGC AS CostAvoidanceAmountGC
		, RFQ.CostAvoidanceAmountAC_Budget AS CostAvoidanceAmountAC_Budget
		, RFQ.CostAvoidanceAmountRC_Budget AS CostAvoidanceAmountRC_Budget
		, RFQ.CostAvoidanceAmountGC_Budget AS CostAvoidanceAmountGC_Budget
		, MaxPurchPriceTC
		, MaxPurchPriceAC
		, MaxPurchPriceRC
		, MaxPurchPriceGC
		, MaxPurchPriceAC_Budget
		, MaxPurchPriceRC_Budget
		, MaxPurchPriceGC_Budget
		, MinPurchPriceTC
		, MinPurchPriceAC
		, MinPurchPriceRC
		, MinPurchPriceGC
		, MinPurchPriceAC_Budget
		, MinPurchPriceRC_Budget
		, MinPurchPriceGC_Budget
		, RFQ.AppliedExchangeRateTC AS AppliedExchangeRateTC
		, RFQ.AppliedExchangeRateRC AS AppliedExchangeRateRC
		, RFQ.AppliedExchangeRateAC AS AppliedExchangeRateAC
		, RFQ.AppliedExchangeRateGC AS AppliedExchangeRateGC

FROM Datastore.RFQ RFQ

LEFT JOIN DataStore.Product P
ON RFQ.CompanyCode = P.CompanyCode
	and RFQ.ProductCode = P.ProductCode

/* ALTER/ADD if Required */ 

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON RFQ.ProductCode = UOM0.ItemNumber
AND RFQ.CompanyCode = UOM0.CompanyCode
AND RFQ.PurchaseUnit = UOM0.FromUOM
AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON RFQ.ProductCode = UOM1.ItemNumber
AND RFQ.CompanyCode = UOM1.CompanyCode
AND RFQ.PurchaseUnit = UOM1.FromUOM
AND UOM1.ToUOM = P.ProductPurchaseUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON RFQ.ProductCode = UOM2.ItemNumber
AND RFQ.CompanyCode = UOM2.CompanyCode
AND RFQ.PurchaseUnit = UOM2.FromUOM
AND UOM2.ToUOM = P.ProductSalesUnit
;
