/****** Object:  View [DWH].[V_DimProductCostBreakdownHierarchy]    Script Date: 03/03/2026 16:26:08 ******/




CREATE OR REPLACE VIEW `DWH`.`V_DimProductCostBreakdownHierarchy` AS


SELECT    CompanyCode
		, Level_1
		, Level_2
		, Level_3
		, Dummy = 1

FROM DataStore.ProductCostBreakDownHierarchy
;
