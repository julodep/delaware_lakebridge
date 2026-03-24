/****** Object:  View [DataStore2].[V_PurchaseDelivery]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore2`.`V_PurchaseDelivery` AS

SELECT	   CONCAT(DLV.PurchaseOrderCode, DLV.CompanyCode, DLV.ProductConfigurationCode, DLV.PurchaseOrderLineNumber
		 , DLV.DeliveryLineNumber) AS DeliveriesIdScreening
		 , DLV.PackingSlipCode AS PackingSlipCode
		   
		   /* Dimensions */	
		 , DLV.PurchaseOrderCode AS PurchaseOrderCode
		 , DLV.CompanyCode AS CompanyCode
		 , DLV.ProductConfigurationCode AS ProductConfigurationCode
		 , DLV.ProductCode AS ProductCode
		 , DLV.OrderSupplierCode AS OrderSupplierCode
		 , DLV.SupplierCode AS SupplierCode
		 , DLV.DeliveryTermsCode AS DeliveryTermsCode
		 , DLV.DeliveryModeCode AS DeliveryModeCode
		  
		   /* Dates */
		 , DLV.ActualDeliveryDate AS ActualDeliveryDate
		 , DLV.RequestedDeliveryDate AS RequestedDeliveryDate
		 , DLV.ConfirmedDeliveryDate AS ConfirmedDeliveryDate
		 
		   /* Delivery Details */
		 , DLV.PurchaseType AS PurchaseType
		 , DLV.PurchaseOrderLineNumber AS PurchaseOrderLineNumber
		 , DLV.DeliveryName AS DeliveryName
		 , DLV.DeliveryLineNumber AS DeliveryLineNumber
		   
		   /* Quantities */
		 , UPPER(DLV.PurchaseUnit) AS PurchaseUnit
		 , COALESCE(CASE WHEN DLV.PurchaseUnit = P.ProductInventoryUnit 
THEN DLV.QuantityOrdered 
ELSE DLV.QuantityOrdered * UOM0.Factor 
END, 0) AS OrderedQuantity_InventoryUnit
		 , COALESCE(CASE WHEN DLV.PurchaseUnit = P.ProductPurchaseUnit 
THEN DLV.QuantityOrdered 
ELSE DLV.QuantityOrdered * UOM1.Factor 
END, 0) AS OrderedQuantity_PurchaseUnit
		 , COALESCE(CASE WHEN DLV.PurchaseUnit = P.ProductSalesUnit 
THEN DLV.QuantityOrdered 
ELSE DLV.QuantityOrdered * UOM2.Factor 
END, 0) AS OrderedQuantity_SalesUnit
		 , COALESCE(CASE WHEN DLV.PurchaseUnit = P.ProductInventoryUnit 
THEN DLV.QuantityDelivered 
ELSE DLV.QuantityDelivered * UOM0.Factor 
END, 0) AS DeliveredQuantity_InventoryUnit
		 , COALESCE(CASE WHEN DLV.PurchaseUnit = P.ProductPurchaseUnit 
THEN DLV.QuantityDelivered 
ELSE DLV.QuantityDelivered * UOM1.Factor 
END, 0) AS DeliveredQuantity_PurchaseUnit
		 , COALESCE(CASE WHEN DLV.PurchaseUnit = P.ProductSalesUnit 
THEN DLV.QuantityDelivered 
ELSE DLV.QuantityDelivered * UOM2.Factor 
END, 0) AS DeliveredQuantity_SalesUnit
		 --ADD Additional Unit of Measures if required!

FROM Datastore.PurchaseDelivery DLV

LEFT JOIN DataStore.Product P
ON DLV.CompanyCode = P.CompanyCode
	and DLV.ProductCode = P.ProductCode

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON DLV.ProductCode = UOM0.ItemNumber
	AND DLV.CompanyCode = UOM0.CompanyCode
	AND UOM0.FromUOM = DLV.PurchaseUnit
	AND UOM0.ToUOM = P.ProductInventoryUnit
LEFT JOIN DataStore.UnitOfMeasure UOM1

ON DLV.ProductCode = UOM1.ItemNumber
	AND DLV.CompanyCode = UOM1.CompanyCode
	AND UOM1.FromUOM = DLV.PurchaseUnit
	AND UOM1.ToUOM = P.ProductPurchaseUnit
LEFT JOIN DataStore.UnitOfMeasure UOM2

ON DLV.ProductCode = UOM2.ItemNumber
	AND DLV.CompanyCode = UOM2.CompanyCode
	AND UOM2.FromUOM = DLV.PurchaseUnit
	AND UOM2.ToUOM = P.ProductSalesUnit
;
