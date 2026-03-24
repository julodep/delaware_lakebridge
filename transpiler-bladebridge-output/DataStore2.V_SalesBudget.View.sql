/****** Object:  View [DataStore2].[V_SalesBudget]    Script Date: 03/03/2026 16:26:09 ******/













CREATE OR REPLACE VIEW `DataStore2`.`V_SalesBudget` AS 


SELECT Concat(SB.CompanyCode,SB.CustomerCode,SB.ProductCode,SB.ForecastDate,SB.InventDimCode) AS SalesBudgetIdScreening
	   
	   --Information on fields
	 , SB.Comment
	  
	   --Dimensions
	 , SB.CompanyCode
	 , SB.ProductCode
	 , SB.ProductGroupCode
	 , SB.CustomerCode
	 , SB.CustomerGroupCode
	 , SB.ForecastModelCode
	 , SB.InventDimCode		
	 , SB.DefaultExchangeRateTypeCode
	 , SB.BudgetExchangeRateTypeCode
	 , SB.TransactionCurrencyCode
	 , SB.AccountingCurrencyCode
	 , SB.ReportingCurrencyCode
	 , SB.GroupCurrencyCode
	  
	   --Financial Dimensions
	 , COALESCE(ADL.MainAccount, '_N/A') AS GLAccountCode
	 , COALESCE(ADL.Intercompany, '_N/A') AS IntercompanyCode
	 --, ISNULL(ADL.CostCenter, '_N/A') AS CostCenterCode	  
	  
	   --Dates
	 , SB.ForecastDate
	  
	   --Measures
	 , SB.SalesUnit
	 , SB.ForecastQuantity

	 /* ADD/ALTER if required */

	 , COALESCE(CASE WHEN SB.SalesUnit = P.ProductInventoryUnit 
THEN SB.ForecastQuantity 
ELSE SB.ForecastQuantity * UOM0.Factor 
END, 0) AS ForecastQuantity_InventoryUnit
	 , COALESCE(CASE WHEN SB.SalesUnit = P.ProductPurchaseUnit 
THEN SB.ForecastQuantity 
ELSE SB.ForecastQuantity * UOM1.Factor 
END, 0) AS ForecastQuantity_PurchaseUnit
	 , COALESCE(CASE WHEN SB.SalesUnit = P.ProductSalesUnit 
THEN SB.ForecastQuantity 
ELSE SB.ForecastQuantity * UOM2.Factor 
END, 0) AS ForecastQuantity_SalesUnit

	   /* GrossSalesAmount */
	 , SB.GrossSalesAmountTC
	 , SB.GrossSalesAmountAC
	 , SB.GrossSalesAmountRC
	 , SB.GrossSalesAmountGC
	 , SB.GrossSalesAmountAC_Budget
	 , SB.GrossSalesAmountRC_Budget
	 , SB.GrossSalesAmountGC_Budget

	   /* CostPrice */
	 , SB.CostPriceTC
	 , SB.CostPriceAC
	 , SB.CostPriceRC
	 , SB.CostPriceGC
	 , SB.CostPriceAC_Budget
	 , SB.CostPriceRC_Budget
	 , SB.CostPriceGC_Budget

	   /* GrossMargin */
	 , SB.GrossMarginTC
	 , SB.GrossMarginAC
	 , SB.GrossMarginRC
	 , SB.GrossMarginGC
	 , SB.GrossMarginAC_Budget
	 , SB.GrossMarginRC_Budget
	 , SB.GrossMarginGC_Budget	 
	 , SB.AppliedExchangeRateTC
	 , SB.AppliedExchangeRateRC
	 , SB.AppliedExchangeRateAC
	 , SB.AppliedExchangeRateGC
	 , SB.AppliedExchangeRateRC_Budget
	 , SB.AppliedExchangeRateAC_Budget
	 , SB.AppliedExchangeRateGC_Budget

FROM DataStore.SalesBudget SB

--Required for analytical dimensions:
LEFT JOIN DataStore.AnalyticalDimensionLedgerSalesAndPurchase ADL
ON SB.DefaultDimension = ADL.DefaultDimensionId

--Required for Unit of Measure:
/* ADD/ALTER where required */

LEFT JOIN DataStore.Product P
ON SB.CompanyCode = P.CompanyCode
	and SB.ProductCode = P.ProductCode

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON SB.ProductCode = UOM0.ItemNumber
	AND SB.CompanyCode = UOM0.CompanyCode
	AND UOM0.FromUOM = SB.SalesUnit
	AND UOM0.ToUOM = P.ProductInventoryUnit
LEFT JOIN DataStore.UnitOfMeasure UOM1

ON SB.ProductCode = UOM1.ItemNumber
	AND SB.CompanyCode = UOM1.CompanyCode
	AND UOM1.FromUOM = SB.SalesUnit
	AND UOM1.ToUOM = P.ProductPurchaseUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON SB.ProductCode = UOM2.ItemNumber
	AND SB.CompanyCode = UOM2.CompanyCode
	AND UOM2.FromUOM = SB.SalesUnit
	AND UOM2.ToUOM = P.ProductSalesUnit
;
