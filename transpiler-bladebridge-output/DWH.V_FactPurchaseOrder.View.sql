/****** Object:  View [DWH].[V_FactPurchaseOrder]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DWH`.`V_FactPurchaseOrder` AS 


SELECT	  UPPER(PurchaseOrderCode) AS PurchaseOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(InventTransCode) AS InventTransCode 
		, UPPER(ProductCode) AS ProductCode
		, UPPER(SupplierCode) AS SupplierCode
		, UPPER(OrderSupplierCode) AS OrderSupplierCode
		, UPPER(DeliveryModeCode) AS DeliveryModeCode
		, UPPER(PaymentTermsCode) AS PaymentTermsCode
		, UPPER(DeliveryTermsCode) AS DeliveryTermsCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, UPPER(InventDimCode) AS InventDimCode

		, ETL.fn_DateKeyInt(CreationDate) AS DimCreationDateId
		, ETL.fn_DateKeyInt(RequestedDeliveryDate) AS DimRequestedDeliveryDateId
		, ETL.fn_DateKeyInt(ConfirmedDeliveryDate) AS DimConfirmedDeliveryDateId

		, PurchaseOrderLineNumber
		, OrderLineNumberCombination
		, PurchaseOrderStatus
		, CAST(DeliveryAddress AS STRING) AS DeliveryAddress
		, PurchaseUnit

		, OrderedQuantity_InventoryUnit
		, OrderedQuantity_PurchaseUnit
		, OrderedQuantity_SalesUnit

		, DeliveredQuantity_InventoryUnit
		, DeliveredQuantity_PurchaseUnit
		, DeliveredQuantity_SalesUnit

		, PurchasePricePerUnitTC
		, PurchasePricePerUnitAC
		, PurchasePricePerUnitRC
		, PurchasePricePerUnitGC

		, PurchasePricePerUnitAC_Budget
		, PurchasePricePerUnitRC_Budget
		, PurchasePricePerUnitGC_Budget

		, GrossPurchaseTC
		, GrossPurchaseAC
		, GrossPurchaseRC
		, GrossPurchaseGC

		, GrossPurchaseAC_Budget
		, GrossPurchaseRC_Budget
		, GrossPurchaseGC_Budget

		, DiscountAmountTC
		, DiscountAmountAC
		, DiscountAmountRC
		, DiscountAmountGC

		, DiscountAmountAC_Budget
		, DiscountAmountRC_Budget
		, DiscountAmountGC_Budget

		, InvoicedPurchaseAmountTC
		, InvoicedPurchaseAmountAC
		, InvoicedPurchaseAmountRC
		, InvoicedPurchaseAmountGC

		, InvoicedPurchaseAmountAC_Budget
		, InvoicedPurchaseAmountRC_Budget
		, InvoicedPurchaseAmountGC_Budget

		, MarkupAmountTC
		, MarkupAmountAC
		, MarkupAmountRC
		, MarkupAmountGC

		, MarkupAmountAC_Budget
		, MarkupAmountRC_Budget
		, MarkupAmountGC_Budget

		, NetPurchaseAmountTC
		, NetPurchaseAmountAC
		, NetPurchaseAmountRC
		, NetPurchaseAmountGC

		, NetPurchaseAmountAC_Budget
		, NetPurchaseAmountRC_Budget
		, NetPurchaseAmountGC_Budget

		, AppliedExchangeRateTC
		, AppliedExchangeRateAC
		, AppliedExchangeRateRC
		, AppliedExchangeRateGC

		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateGC_Budget

		, SurchargeTransportTC
		, SurchargeTransportAC
		, SurchargeTransportRC
		, SurchargeTransportGC

		, SurchargeTransportAC_Budget
		, SurchargeTransportRC_Budget
		, SurchargeTransportGC_Budget

		, SurchargeDeliveryTC
		, SurchargeDeliveryAC
		, SurchargeDeliveryRC
		, SurchargeDeliveryGC

		, SurchargeDeliveryAC_Budget
		, SurchargeDeliveryRC_Budget
		, SurchargeDeliveryGC_Budget

		, SurchargePurchaseTC
		, SurchargePurchaseAC
		, SurchargePurchaseRC
		, SurchargePurchaseGC

		, SurchargePurchaseAC_Budget
		, SurchargePurchaseRC_Budget
		, SurchargePurchaseGC_Budget

		, SurchargeTotalTC
		, SurchargeTotalAC
		, SurchargeTotalRC
		, SurchargeTotalGC

		, SurchargeTotalAC_Budget
		, SurchargeTotalRC_Budget
		, SurchargeTotalGC_Budget

FROM DataStore2.PurchaseOrder
;
