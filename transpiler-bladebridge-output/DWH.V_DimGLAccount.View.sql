/****** Object:  View [DWH].[V_DimGLAccount]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DWH`.`V_DimGLAccount` AS


SELECT	  UPPER(CompanyCode) AS CompanyCode
		, GLAccountId
		, UPPER(GLAccountCode) AS GLAccountCode
		, GLAccountName
		, GLAccountType
		, ChartOfAccountsName
		, MainAccountCategory
		, MainAccountCategoryDescription
		, MainAccountCategoryCodeDescription
		, MainAccountCategorySort
		, IsRevenueFlag
		, IsGrossProfitFlag

FROM DataStore.GLAccount

/* Create unknown member */

UNION ALL

SELECT	DISTINCT UPPER(CompanyCode)
		       , -1
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
			   , '_N/A'
			   , 99999
			   , 'FALSE'
			   , 'FALSE'

FROM DataStore.Company
;
