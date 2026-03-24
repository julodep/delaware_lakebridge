/****** Object:  View [DWH].[V_FactPurchaseDelivery]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DWH`.`V_FactPurchaseDelivery` AS


SELECT    UPPER(PackingSlipCode) AS PackingSlipCode
		, UPPER(PurchaseOrderCode) AS PurchaseOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(ProductConfigurationCode) AS ProductConfigurationCode
		, UPPER(ProductCode) AS ProductCode
		, UPPER(SupplierCode) AS SupplierCode
		, UPPER(OrderSupplierCode) AS OrderSupplierCode
		, UPPER(DeliveryModeCode) AS DeliveryModeCode
		, UPPER(DeliveryTermsCode) AS DeliveryTermsCode
		, CAST(ETL.fn_DateKeyInt(ActualDeliveryDate) AS int) AS DimActualDeliveryDateId
		, CAST(ETL.fn_DateKeyInt(RequestedDeliveryDate) AS int) AS DimRequestedDeliveryDateId
		, CAST(ETL.fn_DateKeyInt(ConfirmedDeliveryDate) AS int) AS DimConfirmedDeliveryDateId
		, PurchaseType
		, PurchaseOrderLineNumber
		, DeliveryName
		, DeliveryLineNumber
		, PurchaseUnit
		, OrderedQuantity_InventoryUnit
		, OrderedQuantity_PurchaseUnit
		, OrderedQuantity_SalesUnit
		, DeliveredQuantity_InventoryUnit
		, DeliveredQuantity_PurchaseUnit
		, DeliveredQuantity_SalesUnit

FROM DataStore2.PurchaseDelivery
;
