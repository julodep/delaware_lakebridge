/****** Object:  View [DWH].[V_FactSalesShipment]    Script Date: 03/03/2026 16:26:09 ******/












CREATE OR REPLACE VIEW `DWH`.`V_FactSalesShipment` AS 


SELECT	  UPPER(CustPackingSlipCode) AS CustPackingSlipCode
		, UPPER(SalesOrderCode) AS SalesOrderCode 
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(ProductCode) AS ProductCode 
		, UPPER(CustomerCode) AS CustomerCode
		, UPPER(OrderCustomerCode) AS OrderCustomerCode
		, UPPER(InventTransCode) AS InventTransCode
		, UPPER(InventDimCode) AS InventDimCode

		, ETL.fn_DateKeyInt(RequestedShippingDate) AS DimRequestedShippingDateId
		, ETL.fn_DateKeyInt(ConfirmedShippingDate) AS DimConfirmedShippingDateId
		, ETL.fn_DateKeyInt(ActualDeliveryDate) AS DimActualDeliveryDateId

		, SalesShipmentIdScreening
		, CustPackingSlipLineNumber
		, CustPackingSlipLineNumberCombination

		, OrderedQuantity_InventoryUnit
		, OrderedQuantity_PurchaseUnit
		, OrderedQuantity_SalesUnit

		, DeliveredQuantity_InventoryUnit
		, DeliveredQuantity_PurchaseUnit
		, DeliveredQuantity_SalesUnit

FROM DataStore2.SalesShipment
;
