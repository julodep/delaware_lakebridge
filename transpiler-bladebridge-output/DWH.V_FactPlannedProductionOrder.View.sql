/****** Object:  View [DWH].[V_FactPlannedProductionOrder]    Script Date: 03/03/2026 16:26:09 ******/







CREATE OR REPLACE VIEW `DWH`.`V_FactPlannedProductionOrder` AS


SELECT  UPPER(PlannedProductionOrderCode) AS PlannedProductionOrderCode
      , UPPER(ProductCode) AS ProductCode
      , UPPER(CompanyCode) AS CompanyCode 
      , UPPER(ProductConfigurationCode) AS ProductConfigurationCode
      , UPPER(ProductionOrderCode) AS ProductionOrderCode
      , ETL.fn_DateKeyInt(RequirementDate) AS DimRequirementDateId
      , RequestedDate 
      , OrderDate
      , DeliveryDate
      , Status
      , LeadTime
	  , InventoryUnit
	  , PurchaseUnit
	  , RequirementQuantity_InventoryUnit
	  , RequirementQuantity_PurchaseUnit
	  , RequirementQuantity_SalesUnit
	  , PurchaseQuantity_InventoryUnit
	  , PurchaseQuantity_PurchaseUnit
	  , PurchaseQuantity_SalesUnit

FROM Datastore2.PlannedProductionOrder
;
