/****** Object:  View [DataStore].[V_ProductCostBreakdownHierarchy]    Script Date: 03/03/2026 16:26:08 ******/





CREATE OR REPLACE VIEW `DataStore`.`V_ProductCostBreakdownHierarchy` AS

/* The following hierarchy is dependent on the levels defined in the costing sheet, which are subsequently loaded using the procedure ETL.CostSheetNodeHierarchy */
;
SELECT    CompanyId AS CompanyCode
		, Level_1
		, Level_2
		, Level_3
		--, Level_4 --> Alter if required
		--, Level_5 --> Alter if required
		--, Level_6 --> Alter if required

FROM (
	
