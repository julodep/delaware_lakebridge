/****** Object:  View [DataStore3].[V_SalesOrder]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DataStore3`.`V_SalesOrder` AS


SELECT	  SalesOrderIdScreening
		, SalesOrderCode
		, SalesOrderLineNumber
		, SalesOrderLineNumberCombination
		, DeliveryAddress
		, CompanyCode
		, InventTransCode
		, InventDimCode
		, DeliveryTermsCode
		, PaymentTermsCode
		, OrderCustomerCode
		, CustomerCode
		, ProductCode
		, SalesOrderStatus
		, DeliveryModeCode
		, DefaultExchangeRateType
		, BudgetExchangeRateType 
		, TransactionCurrencyCode
		, AccountingCurrencyCode 
		, ReportingCurrencyCode 
		, GroupCurrencyCode
		, DocumentStatus
		, OrderTransaction 
		, CreationDate
		, RequestedShippingDate
		, ConfirmedShippingDate
		, RequestedDeliveryDate
		, ConfirmedDeliveryDate
		, FirstShipmentDate
		, LastShipmentDate
		, SalesUnit
		, OrderedQuantity_InventoryUnit
		, OrderedQuantity_PurchaseUnit
		, OrderedQuantity_SalesUnit
		, DeliveredQuantity_InventoryUnit
		, DeliveredQuantity_PurchaseUnit
		, DeliveredQuantity_SalesUnit
		, SalesPricePerUnitTC
		, SalesPricePerUnitAC
		, SalesPricePerUnitRC
		, SalesPricePerUnitGC
		, SalesPricePerUnitAC_Budget
		, SalesPricePerUnitRC_Budget
		, SalesPricePerUnitGC_Budget
		, GrossSalesTC
		, GrossSalesAC
		, GrossSalesRC
		, GrossSalesGC
		, GrossSalesAC_Budget
		, GrossSalesRC_Budget
		, GrossSalesGC_Budget
		, DiscountAmountTC
		, DiscountAmountAC
		, DiscountAmountRC
		, DiscountAmountGC
		, DiscountAmountAC_Budget
		, DiscountAmountRC_Budget
		, DiscountAmountGC_Budget
		, InvoicedSalesAmountTC
		, InvoicedSalesAmountAC
		, InvoicedSalesAmountRC
		, InvoicedSalesAmountGC
		, InvoicedSalesAmountAC_Budget
		, InvoicedSalesAmountRC_Budget
		, InvoicedSalesAmountGC_Budget
		, SurchargeTransportTC
		, SurchargeTransportAC
		, SurchargeTransportRC
		, SurchargeTransportGC
		, SurchargeTransportAC_Budget
		, SurchargeTransportRC_Budget
		, SurchargeTransportGC_Budget
		, SurchargePurchaseTC
		, SurchargePurchaseAC
		, SurchargePurchaseRC
		, SurchargePurchaseGC
		, SurchargePurchaseAC_Budget
		, SurchargePurchaseRC_Budget
		, SurchargePurchaseGC_Budget
		, SurchargeDeliveryTC
		, SurchargeDeliveryAC
		, SurchargeDeliveryRC
		, SurchargeDeliveryGC
		, SurchargeDeliveryAC_Budget
		, SurchargeDeliveryRC_Budget
		, SurchargeDeliveryGC_Budget
		, SurchargeTotalTC
		, SurchargeTotalAC
		, SurchargeTotalRC
		, SurchargeTotalGC
		, SurchargeTotalAC_Budget
		, SurchargeTotalRC_Budget
		, SurchargeTotalGC_Budget
		, NetSalesAmountTC
		, NetSalesAmountAC
		, NetSalesAmountRC
		, NetSalesAmountGC
		, NetSalesAmountAC_Budget
		, NetSalesAmountRC_Budget
		, NetSalesAmountGC_Budget
		, CostOfGoodsSoldTC
		, CostOfGoodsSoldAC
		, CostOfGoodsSoldRC
		, CostOfGoodsSoldGC
		, CostOfGoodsSoldAC_Budget
		, CostOfGoodsSoldRC_Budget
		, CostOfGoodsSoldGC_Budget
		, NetSalesAmountTC - CostOfGoodsSoldTC AS GrossMarginTC
		, NetSalesAmountAC - CostOfGoodsSoldAC AS GrossMarginAC
		, NetSalesAmountRC - CostOfGoodsSoldRC AS GrossMarginRC
		, NetSalesAmountGC - CostOfGoodsSoldGC AS GrossMarginGC
		, NetSalesAmountAC_Budget - CostOfGoodsSoldAC_Budget AS GrossMarginAC_Budget
		, NetSalesAmountRC_Budget - CostOfGoodsSoldRC_Budget AS GrossMarginRC_Budget
		, NetSalesAmountGC_Budget - CostOfGoodsSoldGC_Budget AS GrossMarginGC_Budget
		, AppliedExchangeRateTC
		, AppliedExchangeRateAC
		, AppliedExchangeRateRC
		, AppliedExchangeRateGC
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateGC_Budget
FROM DataStore2.SalesOrder
;
