/****** Object:  View [DataStore2].[V_PlannedProductionOrder]    Script Date: 03/03/2026 16:26:08 ******/








CREATE OR REPLACE VIEW `DataStore2`.`V_PlannedProductionOrder` AS


SELECT PPO.PlannedProductionOrderCode
	   
	   /*Dimensions*/
	 , PPO.ProductCode AS ProductCode
	 , PPO.CompanyCode AS CompanyCode
	 , PPO.ProductConfigurationCode AS ProductConfigurationCode
	 , PPO.ProductionOrderCode AS ProductionOrderCode
	 
	   /*Dates*/
	 , PPO.RequirementDate AS RequirementDate
	 , PPO.RequestedDate AS RequestedDate
	 , PPO.OrderDate AS OrderDate
	 , PPO.DeliveryDate AS DeliveryDate
	 
	   /*Planned Production Order Details*/
	 , PPO.Status AS Status
	 , PPO.LeadTime AS LeadTime
	 
	   /*Key Figures*/
	   /* ADD/ALTER if Required */
	 , PPO.InventoryUnit AS InventoryUnit
	 , PPO.PurchaseUnit AS PurchaseUnit
	 , COALESCE(PPO.RequirementQuantity, 0) AS RequirementQuantity_InventoryUnit
	 , COALESCE(CASE WHEN PPO.InventoryUnit = P.ProductPurchaseUnit THEN PPO.RequirementQuantity ELSE PPO.RequirementQuantity * UOM1_Invent.Factor END, 0) AS RequirementQuantity_PurchaseUnit -- Quantity Purchase Unit
	 , COALESCE(CASE WHEN PPO.InventoryUnit = P.ProductSalesUnit THEN PPO.RequirementQuantity ELSE PPO.RequirementQuantity * UOM2_Invent.Factor END, 0) AS RequirementQuantity_SalesUnit -- Quantity Sales Unit
	 , COALESCE(CASE WHEN PPO.PurchaseUnit = P.ProductInventoryUnit THEN PPO.PurchaseQuantity ELSE PPO.PurchaseQuantity * UOM0_Purch.Factor END, 0) AS PurchaseQuantity_InventoryUnit	-- Quantity Inventory Unit
	 , COALESCE(PPO.PurchaseQuantity, 0) AS PurchaseQuantity_PurchaseUnit	-- Quantity Purchase Unit
	 , COALESCE(CASE WHEN PPO.PurchaseUnit = P.ProductSalesUnit THEN PPO.PurchaseQuantity ELSE PPO.PurchaseQuantity * UOM2_Purch.Factor END, 0) AS PurchaseQuantity_SalesUnit	-- Quantity Sales Unit

FROM Datastore.PlannedProductionOrder PPO

LEFT JOIN DataStore.Product P
ON PPO.CompanyCode = P.CompanyCode
	and PPO.ProductCode = P.ProductCode

LEFT JOIN DataStore.UnitOfMeasure UOM1_Invent
ON PPO.ProductCode = UOM1_Invent.ItemNumber
AND PPO.CompanyCode = UOM1_Invent.CompanyCode
AND PPO.InventoryUnit = UOM1_Invent.FromUOM
AND UOM1_Invent.ToUOM = P.ProductPurchaseUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2_Invent
ON PPO.ProductCode = UOM2_Invent.ItemNumber
AND PPO.CompanyCode = UOM2_Invent.CompanyCode
AND PPO.InventoryUnit = UOM2_Invent.FromUOM
AND UOM2_Invent.ToUOM = P.ProductSalesUnit

LEFT JOIN DataStore.UnitOfMeasure UOM0_Purch
ON PPO.ProductCode = UOM0_Purch.ItemNumber
AND PPO.CompanyCode = UOM0_Purch.CompanyCode
AND PPO.PurchaseUnit = UOM0_Purch.FromUOM
AND UOM0_Purch.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2_Purch
ON PPO.ProductCode = UOM2_Purch.ItemNumber
AND PPO.CompanyCode = UOM2_Purch.CompanyCode
AND PPO.PurchaseUnit = UOM2_Purch.FromUOM
AND UOM2_Purch.ToUOM = P.ProductSalesUnit
;
