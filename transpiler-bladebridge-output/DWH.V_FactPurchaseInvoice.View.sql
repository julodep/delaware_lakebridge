/****** Object:  View [DWH].[V_FactPurchaseInvoice]    Script Date: 03/03/2026 16:26:08 ******/


















CREATE OR REPLACE VIEW `DWH`.`V_FactPurchaseInvoice` AS 


SELECT	  --UPPER(PurchaseInvoiceCode) AS PurchaseInvoiceCode
		CAST(REPLACE(CAST(UPPER(PurchaseInvoiceCode) AS STRING), '?', '') AS STRING) AS PurchaseInvoiceCode
		, UPPER(CompanyCode) AS CompanyCode 
		, UPPER(PurchaseOrderCode) AS PurchaseOrderCode
		, UPPER(InventTransCode) AS InventTransCode
		, UPPER(InventDimCode) AS InventDimCode
		, UPPER(SupplierCode) AS SupplierCode
		, UPPER(ProductCode) AS ProductCode 
		, UPPER(InternalInvoiceCode) AS InternalInvoiceCode
		, UPPER(PaymentTermsCode) AS PaymentTermsCode
		, UPPER(DeliveryModeCode) AS DeliveryModeCode
		, UPPER(DeliveryTermsCode) AS DeliveryTermsCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, UPPER(GLAccountCode) AS GLAccountCode
		, UPPER(IntercompanyCode) AS IntercompanyCode
		--, UPPER(CostCenterCode) AS CostCenterCode
		, UPPER(BusinessSegmentCode) AS BusinessSegmentCode
		, UPPER(DepartmentCode) AS DepartmentCode
		, UPPER(EndCustomerCode) AS EndCustomerCode
		, UPPER(LocationCode) AS LocationCode
		, UPPER(ShipmentContractCode) AS ShipmentContractCode
		, UPPER(LocalAccountCode) AS LocalAccountCode
		, UPPER(ProductFDCode) AS ProductFDCode
	   

		, ETL.fn_DateKeyInt(InvoiceDate) AS DimInvoiceDateId
		, ETL.fn_MonthKeyInt(InvoiceDate) AS InvoiceMonthId

		, PurchaseInvoiceLineNumber 
		, InvoiceLineNumberCombination
		, PurchaseOrderStatus
		, TaxWriteCode
		, PurchaseUnit

		, InvoicedQuantity_InventoryUnit
		, InvoicedQuantity_PurchaseUnit
		, InvoicedQuantity_SalesUnit

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

		, SurchargeTotalTC
		, SurchargeTotalAC
		, SurchargeTotalRC
		, SurchargeTotalGC

		, SurchargeTotalAC_Budget
		, SurchargeTotalRC_Budget
		, SurchargeTotalGC_Budget

FROM DataStore2.PurchaseInvoice
;
