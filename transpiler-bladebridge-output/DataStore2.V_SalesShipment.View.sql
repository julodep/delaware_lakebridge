/****** Object:  View [DataStore2].[V_SalesShipment]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DataStore2`.`V_SalesShipment` AS 


SELECT 
		CONCAT(SS.CustPackingSlipCode,SS.CustPackingSlipLineNumber,SS.CompanyCode,SS.SalesOrderCode,SS.ProductCode) AS SalesShipmentIdScreening
	
	--Dimensions
		, SS.CustPackingSlipCode
		, SS.CustPackingSlipLineNumber
		, SS.CustPackingSlipLineNumberCombination
		, SS.SalesOrderCode
		, SS.CompanyCode
		, SS.ProductCode
		, SS.CustomerCode
		, SS.OrderCustomerCode
		, SS.InventTransCode
		, SS.InventDimCode
		, UPPER(SS.SalesUnit) AS SalesUnit
	
	--Dates
		, SS.RequestedShippingDate AS RequestedShippingDate
		, SS.ConfirmedShippingDate AS ConfirmedShippingDate
		, SS.ActualDeliveryDate AS ActualDeliveryDate
	
	--Quantities:
		, COALESCE(CASE WHEN SS.SalesUnit = P.ProductInventoryUnit 
THEN SS.OrderedQuantity 
ELSE SS.OrderedQuantity * UOM0.Factor 
END, 0) AS OrderedQuantity_InventoryUnit
		, COALESCE(CASE WHEN SS.SalesUnit = P.ProductPurchaseUnit 
THEN SS.OrderedQuantity 
ELSE SS.OrderedQuantity * UOM1.Factor 
END, 0) AS OrderedQuantity_PurchaseUnit
		, COALESCE(CASE WHEN SS.SalesUnit = P.ProductSalesUnit 
THEN SS.OrderedQuantity 
ELSE SS.OrderedQuantity * UOM2.Factor 
END, 0) AS OrderedQuantity_SalesUnit
		, COALESCE(CASE WHEN SS.SalesUnit = P.ProductInventoryUnit 
THEN SS.DeliveredQuantity 
ELSE SS.DeliveredQuantity * UOM0.Factor 
END, 0) AS DeliveredQuantity_InventoryUnit
		, COALESCE(CASE WHEN SS.SalesUnit = P.ProductPurchaseUnit 
THEN SS.DeliveredQuantity 
ELSE SS.DeliveredQuantity * UOM1.Factor 
END, 0) AS DeliveredQuantity_PurchaseUnit
		, COALESCE(CASE WHEN SS.SalesUnit = P.ProductSalesUnit 
THEN SS.DeliveredQuantity 
ELSE SS.DeliveredQuantity * UOM2.Factor 
END, 0) AS DeliveredQuantity_SalesUnit


FROM DataStore.SalesShipment SS

LEFT JOIN DataStore.Product P
ON SS.CompanyCode = P.CompanyCode
	and SS.ProductCode = P.ProductCode

LEFT JOIN DataStore.UnitOfMeasure UOM0
ON SS.ProductCode = UOM0.ItemNumber
	AND SS.CompanyCode = UOM0.CompanyCode
	AND UOM0.FromUOM = SS.SalesUnit
	AND UOM0.ToUOM = P.ProductInventoryUnit

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON SS.ProductCode = UOM1.ItemNumber
	AND SS.CompanyCode = UOM1.CompanyCode
	AND UOM1.FromUOM = SS.SalesUnit
	AND UOM1.ToUOM = P.ProductPurchaseUnit

LEFT JOIN DataStore.UnitOfMeasure UOM2
ON SS.ProductCode = UOM2.ItemNumber
	AND SS.CompanyCode = UOM2.CompanyCode
	AND UOM2.FromUOM = SS.SalesUnit
	AND UOM2.ToUOM = P.ProductSalesUnit
;
