/****** Object:  View [DWH].[V_FactSalesBudget]    Script Date: 03/03/2026 16:26:08 ******/














CREATE OR REPLACE VIEW `DWH`.`V_FactSalesBudget` AS 


SELECT	  UPPER(CompanyCode) AS CompanyCode
		, UPPER(ProductCode) AS ProductCode
		, UPPER(CustomerCode) AS CustomerCode 
		, UPPER(ForecastModelCode) AS ForecastModelCode
		, UPPER(InventDimCode) AS InventDimCode
		, UPPER(GLAccountCode) AS GLAccountCode
		, UPPER(IntercompanyCode) AS IntercompanyCode
		--, UPPER(CostCenterCode) AS CostCenterCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode

		, ETL.fn_DateKeyInt(ForecastDate) AS DimForecastDateId
	
		, ProductGroupCode
		, CustomerGroupCode
		, Comment
		, SalesUnit

	    , ForecastQuantity_InventoryUnit
		, ForecastQuantity_PurchaseUnit
		, ForecastQuantity_SalesUnit

		, GrossSalesAmountTC
		, GrossSalesAmountAC
		, GrossSalesAmountRC
		, GrossSalesAmountGC

		, GrossSalesAmountAC_Budget
		, GrossSalesAmountRC_Budget
		, GrossSalesAmountGC_Budget

		, CostPriceTC
		, CostPriceAC
		, CostPriceRC
		, CostPriceGC

		, CostPriceAC_Budget
		, CostPriceRC_Budget
		, CostPriceGC_Budget

		, GrossMarginTC
		, GrossMarginAC
		, GrossMarginRC
		, GrossMarginGC

		, GrossMarginAC_Budget
		, GrossMarginRC_Budget
		, GrossMarginGC_Budget

		, AppliedExchangeRateTC
		, AppliedExchangeRateRC
		, AppliedExchangeRateAC
		, AppliedExchangeRateGC

		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateGC_Budget

FROM DataStore2.SalesBudget
;
