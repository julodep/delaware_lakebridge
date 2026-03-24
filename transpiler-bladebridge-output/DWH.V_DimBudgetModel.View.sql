/****** Object:  View [DWH].[V_DimBudgetModel]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimBudgetModel` AS
 

SELECT    UPPER(CompanyCode) AS CompanyCode
		, UPPER(BudgetModelCode) AS BudgetModelCode
		, BudgetModelName

FROM DataStore.BudgetModel

/* Create Unknown Member */

UNION ALL  

SELECT	DISTINCT UPPER(CompanyCode)
		       , '_N/A'
		       , '_N/A'
FROM DataStore.Company
;
