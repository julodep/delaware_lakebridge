/****** Object:  View [DWH].[V_FactInventory]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DWH`.`V_FactInventory` AS


SELECT    UPPER(ProductCode) AS ProductCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(ProductConfigurationCode) AS ProductConfigurationCode
		, UPPER(BatchCode) AS BatchCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, ETL.fn_DateKeyInt(ReportDate) AS DimReportDateId
		, CAST(ReportDate as Date) AS ReportDate
		, IF(CAST(ReportDate as Date) = LAST_DAY(ReportDate), 1, 0) AS IsEndOfMonth
		, InventoryUnit
		, StockQuantity_InventoryUnit
		, StockQuantity_PurchaseUnit
		, StockQuantity_SalesUnit
		, StockValueAC
		, StockValueRC
		, StockValueGC
		, StockValueAC_Budget
		, StockValueRC_Budget
		, StockValueGC_Budget
		, AppliedExchangeRateRC
		, AppliedExchangeRateAC
		, AppliedExchangeRateGC
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateGC_Budget

FROM Datastore2.Inventory
;
