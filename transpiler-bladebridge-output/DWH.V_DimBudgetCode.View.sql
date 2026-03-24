/****** Object:  View [DWH].[V_DimBudgetCode]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DWH`.`V_DimBudgetCode` AS 


SELECT    UPPER(CompanyCode) AS CompanyCode
		, UPPER(BudgetCodeName) AS BudgetCodeName
		, BudgetCodeDescription
		, BudgetTransactionCode

FROM DataStore.BudgetCode

/* Create unknown member */

UNION ALL

SELECT	DISTINCT UPPER(CompanyCode)
		       , '_N/A'
		       , '_N/A'
		       , -1
FROM DataStore.Company
;
