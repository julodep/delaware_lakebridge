/****** Object:  View [DWH].[V_FactSalesOrder]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DWH`.`V_FactSalesOrder` AS


SELECT	  UPPER(SalesOrderCode) AS SalesOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(InventTransCode) AS InventTransCode
		, UPPER(InventDimCode) AS InventDimCode
		, UPPER(DeliveryTermsCode) AS DeliveryTermsCode
		, UPPER(PaymentTermsCode) AS PaymentTermsCode
		, UPPER(ProductCode) AS ProductCode
		, UPPER(DeliveryModeCode) AS DeliveryModeCode
		, UPPER(DefaultExchangeRateType) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateType)  AS BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, UPPER(CustomerCode) AS CustomerCode
		, UPPER(OrderCustomerCode) AS OrderCustomerCode

		, ETL.fn_DateKeyInt(CreationDate) AS DimCreationDateId
		, ETL.fn_DateKeyInt(RequestedShippingDate) AS DimRequestedShippingDateId
		, ETL.fn_DateKeyInt(ConfirmedShippingDate) AS DimConfirmedShippingDateId
		, ETL.fn_DateKeyInt(RequestedDeliveryDate) AS DimRequestedDeliveryDateId
		, ETL.fn_DateKeyInt(ConfirmedDeliveryDate) AS DimConfirmedDeliveryDateId
		, ETL.fn_DateKeyInt(LastShipmentDate) AS DimLastShipmentDateId

		, SalesOrderStatus
		, SalesOrderLineNumber
		, SalesOrderLineNumberCombination
		, DeliveryAddress 
		, DocumentStatus
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

		, SurchargeDeliveryAC
		, SurchargeDeliveryAC_Budget
		, SurchargeDeliveryGC
		, SurchargeDeliveryGC_Budget
		, SurchargeDeliveryRC
		, SurchargeDeliveryRC_Budget
		, SurchargeDeliveryTC

		, SurchargePurchaseAC
		, SurchargePurchaseAC_Budget
		, SurchargePurchaseGC
		, SurchargePurchaseGC_Budget
		, SurchargePurchaseRC
		, SurchargePurchaseRC_Budget
		, SurchargePurchaseTC

		, SurchargeTotalAC
		, SurchargeTotalAC_Budget
		, SurchargeTotalGC
		, SurchargeTotalGC_Budget
		, SurchargeTotalRC
		, SurchargeTotalRC_Budget
		, SurchargeTotalTC

		, SurchargeTransportAC
		, SurchargeTransportAC_Budget
		, SurchargeTransportGC
		, SurchargeTransportGC_Budget
		, SurchargeTransportRC
		, SurchargeTransportRC_Budget
		, SurchargeTransportTC

		, InvoicedSalesAmountTC
		, InvoicedSalesAmountAC
		, InvoicedSalesAmountRC
		, InvoicedSalesAmountGC

		, InvoicedSalesAmountAC_Budget
		, InvoicedSalesAmountRC_Budget
		, InvoicedSalesAmountGC_Budget

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

		, GrossMarginTC
		, GrossMarginAC
		, GrossMarginRC
		, GrossMarginGC

		, GrossMarginAC_Budget
		, GrossMarginRC_Budget
		, GrossMarginGC_Budget

		, AppliedExchangeRateTC
		, AppliedExchangeRateAC
		, AppliedExchangeRateRC
		, AppliedExchangeRateGC

		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateGC_Budget

FROM DataStore3.SalesOrder
;
