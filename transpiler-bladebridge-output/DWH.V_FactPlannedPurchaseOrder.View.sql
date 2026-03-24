/****** Object:  View [DWH].[V_FactPlannedPurchaseOrder]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DWH`.`V_FactPlannedPurchaseOrder` AS


SELECT  UPPER(PlannedPurchaseOrderCode) AS PlannedPurchaseOrderCode
      , UPPER(ProductCode) AS ProductCode
      , UPPER(SupplierCode) AS SupplierCode
      , UPPER(CompanyCode) AS CompanyCode 
      , UPPER(ProductConfigurationCode) AS ProductConfigurationCode
      , UPPER(PurchaseOrderCode) AS PurchaseOrderCode
      , ETL.fn_DateKeyInt(RequirementDate) AS DimRequirementDateId
      , ETL.fn_DateKeyInt(RequestedDate) AS DimRequestedDateId
      , ETL.fn_DateKeyInt(OrderDate) AS DimOrderDateId
      , ETL.fn_DateKeyInt(DeliveryDate) AS DimDeliveryDateId
      , Status
      , LeadTime
      , InventoryUnit
	  , RequirementQuantity_InventoryUnit
	  , RequirementQuantity_PurchaseUnit
	  , RequirementQuantity_SalesUnit
      , PurchaseUnit
	  , PurchaseQuantity_InventoryUnit
	  , PurchaseQuantity_PurchaseUnit
	  , PurchaseQuantity_SalesUnit
	  
FROM Datastore2.PlannedPurchaseOrder
;
