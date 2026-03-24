/****** Object:  View [DWH].[V_FactProductCostBreakdownTheoretical]    Script Date: 03/03/2026 16:26:09 ******/





CREATE OR REPLACE VIEW `DWH`.`V_FactProductCostBreakdownTheoretical` AS 


SELECT	  CompanyCode
		, FinishedProductCode
		, ProductCode
		, InventDimCode
		, CalculationType
		, CalculationCode
		, PriceType
		, IsCalculatedPrice
		, IsMaxCalculation
		, IsMaxPrice
		, Levelling
		, Resource_
		, CostGroupCode
		, CostPriceUnitSymbol
		, CostPriceCalculationNumber
		, CostPriceModel
		, CostPriceVersion
		, CostPriceType
		, DimPriceCalculationDateId = ETL.fn_DateKeyInt(CalculationDate)
		, IsActivePrice
		, AccountingCurrencyCode
		, ReportingCurrencyCode
		, GroupCurrencyCode
		, DefaultExchangeRateTypeCode
		, BudgetExchangeRateTypeCode
		, Level_1
		, Level_2
		, Level_3
		, ProductCostPriceAC
		, ProductCostPriceRC
		, ProductCostPriceGC
		, ProductCostPriceRC_Budget
		, ProductCostPriceGC_Budget
		, ComponentCostPriceAC
		, ComponentCostPriceRC
		, ComponentCostPriceGC
		, ComponentCostPriceRC_Budget
		, ComponentCostPriceGC_Budget

FROM DataStore6.ProductCostBreakdownTheoretical
;
