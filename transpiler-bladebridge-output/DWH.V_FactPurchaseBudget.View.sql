/****** Object:  View [DWH].[V_FactPurchaseBudget]    Script Date: 03/03/2026 16:26:09 ******/











CREATE OR REPLACE VIEW `DWH`.`V_FactPurchaseBudget` AS


SELECT    ForecastModelCode AS ForecastModelCode
		, CompanyCode AS CompanyCode
		, InventDimCode AS InventDimCode
		, ProductCode AS ProductCode
		, SupplierCode AS SupplierCode
		, DefaultExchangeRateTypeCode 
		, BudgetExchangeRateTypeCode
		, TransactionCurrencyCode AS TransactionCurrencyCode
		, AccountingCurrencyCode AS AccountingCurrencyCode
		, ReportingCurrencyCode AS ReportingCurrencyCode
		, GroupCurrencyCode AS GroupCurrencyCode
		, ETL.fn_DateKeyInt(BudgetDate) AS DimBudgetDateId
		, PurchaseUnit
		, BudgetQuantity_InventoryUnit
		, BudgetQuantity_PurchaseUnit
		, BudgetQuantity_SalesUnit
		, PurchUnitPriceTC
		, PurchUnitPriceAC
		, PurchUnitPriceRC
		, PurchUnitPriceGC
		, PurchUnitPriceAC_Budget
		, PurchUnitPriceRC_Budget
		, PurchUnitPriceGC_Budget
		, BudgetAmountTC
		, BudgetAmountAC
		, BudgetAmountRC
		, BudgetAmountGC
		, BudgetAmountAC_Budget
		, BudgetAmountRC_Budget
		, BudgetAmountGC_Budget
		, AppliedExchangeRateTC
		, AppliedExchangeRateRC
		, AppliedExchangeRateAC
		, AppliedExchangeRateGC
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateGC_Budget

FROM DataStore2.PurchaseBudget
;
