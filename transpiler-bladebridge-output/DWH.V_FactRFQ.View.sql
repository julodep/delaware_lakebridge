/****** Object:  View [DWH].[V_FactRFQ]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DWH`.`V_FactRFQ` AS


SELECT    UPPER(RFQCaseCode) AS RFQCaseCode
		, UPPER(RFQCode) AS RFQCode
		, UPPER(SupplierCode) AS SupplierCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(ProductConfigurationCode) AS ProductConfigurationCode
		, UPPER(ProductCode) AS ProductCode
		, UPPER(DeliveryModeCode) AS DeliveryModeCode
		, UPPER(DeliveryTermsCode) AS DeliveryTermsCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode

		, ETL.fn_DateKeyInt(DeliveryDate) AS DimDeliveryDateId
		, ETL.fn_DateKeyInt(ExpiryDate) AS DimExpiryDateId
		, ETL.fn_DateKeyInt(CreatedDate) AS DimCreatedDateId

		, RFQCaseName
		, RFQCaseLineNumber
		, RFQName
		, RFQLineNumber
		, RFQCaseStatusLow
		, RFQCaseStatusHigh
		, RFQLineStatus
		, PurchUnit AS PurchaseUnit

		, OrderedQuantity_InventoryUnit
		, OrderedQuantity_PurchaseUnit
		, OrderedQuantity_SalesUnit

		, PurchPriceTC, PurchPriceAC
		, PurchPriceRC, PurchPriceGC

		, PurchPriceAC_Budget
		, PurchPriceRC_Budget
		, PurchPriceGC_Budget

		, RFQLineAmountTC
		, RFQLineAmountAC
		, RFQLineAmountRC
		, RFQLineAmountGC

		, RFQLineAmountAC_Budget
		, RFQLineAmountRC_Budget
		, RFQLineAmountGC_Budget

		, CostAvoidanceAmountTC
		, CostAvoidanceAmountAC
		, CostAvoidanceAmountRC
		, CostAvoidanceAmountGC

		, CostAvoidanceAmountAC_Budget
		, CostAvoidanceAmountRC_Budget
		, CostAvoidanceAmountGC_Budget

		, MaxPurchPriceTC
		, MaxPurchPriceAC
		, MaxPurchPriceRC
		, MaxPurchPriceGC

		, MaxPurchPriceAC_Budget
		, MaxPurchPriceRC_Budget
		, MaxPurchPriceGC_Budget

		, MinPurchPriceTC
		, MinPurchPriceAC
		, MinPurchPriceRC
		, MinPurchPriceGC

		, MinPurchPriceAC_Budget
		, MinPurchPriceRC_Budget
		, MinPurchPriceGC_Budget

		, AppliedExchangeRateTC
		, AppliedExchangeRateRC
		, AppliedExchangeRateAC
		, AppliedExchangeRateGC

FROM Datastore2.RFQ
;
