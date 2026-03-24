/****** Object:  View [DataStore2].[V_PurchaseBudget]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DataStore2`.`V_PurchaseBudget` AS


SELECT CONCAT(PB.ForecastModelCode,PB.CompanyCode,PB.BudgetDate) AS PurchaseBudgetIdScreening
	   
	   /*Dimensions*/
	 , PB.ForecastModelCode AS ForecastModelCode
	 , PB.CompanyCode AS CompanyCode
	   --PB.ProductCategoryCode AS ProductCategoryCode
	 , PB.DefaultExchangeRateTypeCode AS DefaultExchangeRateTypeCode
	 , PB.BudgetExchangeRateTypeCode AS BudgetExchangeRateTypeCode
	 , PB.TransactionCurrencyCode AS TransactionCurrencyCode
	 , PB.AccountingCurrencyCode AS AccountingCurrencyCode
	 , PB.ReportingCurrencyCode AS ReportingCurrencyCode
	 , PB.GroupCurrencyCode AS GroupCurrencyCode
	 , PB.ProductCode AS ProductCode
	 , PB.SupplierCode AS SupplierCode
	 , PB.InventDimCode AS InventDimCode
	  
	   /*Dates*/
	 , PB.BudgetDate
	  
	   /*Measures*/	
	 , PB.PurchaseUnit AS PurchaseUnit
	 , COALESCE(CASE WHEN PB.PurchaseUnit = P.ProductInventoryUnit 
THEN PB.BudgetQuantity 
ELSE PB.BudgetQuantity * UOM0.Factor 
END, 0) AS BudgetQuantity_InventoryUnit
	 , COALESCE(CASE WHEN PB.PurchaseUnit = P.ProductPurchaseUnit 
THEN PB.BudgetQuantity 
ELSE PB.BudgetQuantity * UOM1.Factor 
END, 0) AS BudgetQuantity_PurchaseUnit
	 , COALESCE(CASE WHEN PB.PurchaseUnit = P.ProductSalesUnit 
THEN PB.BudgetQuantity 
ELSE PB.BudgetQuantity * UOM2.Factor 
END, 0) AS BudgetQuantity_SalesUnit
	 , PB.PurchUnitPriceTC AS PurchUnitPriceTC
	 , PB.PurchUnitPriceAC AS PurchUnitPriceAC
	 , PB.PurchUnitPriceRC AS PurchUnitPriceRC
	 , PB.PurchUnitPriceGC AS PurchUnitPriceGC
	 , PB.PurchUnitPriceAC_Budget AS PurchUnitPriceAC_Budget
	 , PB.PurchUnitPriceRC_Budget AS PurchUnitPriceRC_Budget
	 , PB.PurchUnitPriceGC_Budget AS PurchUnitPriceGC_Budget
	 , PB.BudgetAmountTC AS BudgetAmountTC
	 , PB.BudgetAmountAC AS BudgetAmountAC
	 , PB.BudgetAmountRC AS BudgetAmountRC
	 , PB.BudgetAmountGC AS BudgetAmountGC
	 , PB.BudgetAmountAC_Budget AS BudgetAmountAC_Budget
	 , PB.BudgetAmountRC_Budget AS BudgetAmountRC_Budget
	 , PB.BudgetAmountGC_Budget AS BudgetAmountGC_Budget
	 , PB.AppliedExchangeRateTC AS AppliedExchangeRateTC
	 , PB.AppliedExchangeRateRC AS AppliedExchangeRateRC
	 , PB.AppliedExchangeRateAC AS AppliedExchangeRateAC
	 , PB.AppliedExchangeRateGC AS AppliedExchangeRateGC
	 , PB.AppliedExchangeRateRC_Budget AS AppliedExchangeRateRC_Budget
	 , PB.AppliedExchangeRateAC_Budget AS AppliedExchangeRateAC_Budget
	 , PB.AppliedExchangeRateGC_Budget AS AppliedExchangeRateGC_Budget	 

FROM DataStore.PurchaseBudget PB

LEFT JOIN DataStore.Product P
ON PB.CompanyCode = P.CompanyCode
	and PB.ProductCode = P.ProductCode

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON PB.ProductCode = UOM0.ItemNumber
	AND PB.CompanyCode = UOM0.CompanyCode
	AND UOM0.FromUOM = PB.PurchaseUnit
	AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON PB.ProductCode = UOM1.ItemNumber
	AND PB.CompanyCode = UOM1.CompanyCode
	AND UOM1.FromUOM = PB.PurchaseUnit
	AND UOM1.ToUOM = P.ProductPurchaseUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON PB.ProductCode = UOM2.ItemNumber
	AND PB.CompanyCode = UOM2.CompanyCode
	AND UOM2.FromUOM = PB.PurchaseUnit
	AND UOM2.ToUOM = P.ProductSalesUnit
;
