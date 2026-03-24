/****** Object:  View [DataStore2].[V_Inventory]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore2`.`V_Inventory` AS


SELECT	/* Dimensions */
		   I.ProductCode AS ProductCode                   
		 , I.ProductCode AS ProductCodeScreening          
		 , I.CompanyCode AS CompanyCode                     
		 , I.ProductConfigurationCode AS ProductConfigurationCode        
		 , I.BatchCode AS BatchCode                       
		 , I.ReportDate AS ReportDate                    
		 , I.DefaultExchangeRateTypeCode AS DefaultExchangeRateTypeCode     
		 , I.BudgetExchangeRateTypeCode AS BudgetExchangeRateTypeCode              
		 , I.AccountingCurrencyCode AS AccountingCurrencyCode          
		 , I.ReportingCurrencyCode AS ReportingCurrencyCode           
		 , I.GroupCurrencyCode AS GroupCurrencyCode
		
		/* Quantities */                    
		 , UPPER(I.InventoryUnit) AS InventoryUnit                    
		 , COALESCE(CASE WHEN I.InventoryUnit = P.ProductInventoryUnit THEN I.StockQuantity ELSE I.StockQuantity * UOM0.Factor END, 0) AS StockQuantity_InventoryUnit
		 , COALESCE(CASE WHEN I.InventoryUnit = P.ProductPurchaseUnit THEN I.StockQuantity ELSE I.StockQuantity * UOM1.Factor END, 0) AS StockQuantity_PurchaseUnit
		 , COALESCE(CASE WHEN I.InventoryUnit = P.ProductSalesUnit THEN I.StockQuantity ELSE I.StockQuantity * UOM2.Factor END, 0) AS StockQuantity_SalesUnit
		 --ADD Additional Unit of Measures if required!
		
		/* Values */                
		 , I.StockValueAC AS StockValueAC                  
		 , I.StockValueRC AS StockValueRC                  
		 , I.StockValueGC AS StockValueGC                  
		 , I.StockValueAC_Budget AS StockValueAC_Budget           
		 , I.StockValueRC_Budget AS StockValueRC_Budget           
		 , I.StockValueGC_Budget AS StockValueGC_Budget                 
		 , I.AppliedExchangeRateRC AS AppliedExchangeRateRC         
		 , I.AppliedExchangeRateAC AS AppliedExchangeRateAC         
		 , I.AppliedExchangeRateGC AS AppliedExchangeRateGC         
		 , I.AppliedExchangeRateRC_Budget AS AppliedExchangeRateRC_Budget  
		 , I.AppliedExchangeRateAC_Budget AS AppliedExchangeRateAC_Budget  
		 , I.AppliedExchangeRateGC_Budget AS AppliedExchangeRateGC_Budget

FROM Datastore.Inventory AS I

LEFT JOIN DataStore.Product P
ON I.CompanyCode = P.CompanyCode
	and I.ProductCode = P.ProductCode

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON I.ProductCode = UOM0.ItemNumber
	AND I.CompanyCode = UOM0.CompanyCode
	AND UOM0.FromUOM = I.InventoryUnit
	AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON I.ProductCode = UOM1.ItemNumber
	AND I.CompanyCode = UOM1.CompanyCode
	AND UOM1.FromUOM = I.InventoryUnit
	AND UOM1.ToUOM = P.ProductPurchaseUnit
LEFT JOIN DataStore.UnitOfMeasure UOM2

ON I.ProductCode = UOM2.ItemNumber
	AND I.CompanyCode = UOM2.CompanyCode
	AND UOM2.FromUOM = I.InventoryUnit
	AND UOM2.ToUOM = P.ProductSalesUnit
;
