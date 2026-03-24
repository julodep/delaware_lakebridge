/****** Object:  View [DataStore].[V_GLAccount]    Script Date: 03/03/2026 16:26:09 ******/
















CREATE OR REPLACE VIEW `DataStore`.`V_GLAccount` AS


SELECT    UPPER(LES.`Name`) AS CompanyCode
		, MAS.MainAccountRecId AS GLAccountId
		, MAS.MainAccountId AS GLAccountCode
		, COALESCE(MAS.`Name`, '_N/A') AS GLAccountName
		, CAST(COALESCE(SM.Name, '_N/A') AS STRING) AS GLAccountType
		, COALESCE(NULLIF(LCOAS.ChartOfAccounts, ''), '_N/A') AS ChartOfAccountsName
		, COALESCE(NULLIF(MACS.MainAccountCategory, ''), '_N/A') AS MainAccountCategory
		, COALESCE(NULLIF(MACS.Description, ''), '_N/A') AS MainAccountCategoryDescription
		, COALESCE(NULLIF(MACS.MainAccountCategory, ''), '_N/A') || ' ' || COALESCE(NULLIF(MACS.Description, ''), '_N/A') AS MainAccountCategoryCodeDescription
		, COALESCE(NULLIF(MACS.ReferenceId, ''), 99999) AS MainAccountCategorySort
		, CAST (CASE WHEN MAS.MAINACCOUNTID like '3%' THEN 'TRUE'  ELSE 'FALSE' END AS BOOLEAN) AS IsRevenueFlag
		, CAST (CASE WHEN MAS.MAINACCOUNTID like '4%' OR  MAS.MAINACCOUNTID like '3%' THEN 'TRUE'  ELSE 'FALSE' END AS BOOLEAN) AS IsGrossProfitFlag

FROM dbo.SMRBIMainAccountStaging MAS

INNER JOIN dbo.SMRBILedgerStaging LES
ON MAS.ChartOfAccountsRecId = LES.ChartOfAccountsRecId

LEFT JOIN (SELECT DISTINCT * FROM dbo.SMRBILedgerChartOfAccountsStaging) LCOAS
ON LCOAS.ChartOfAccountsRecId = LES.ChartOfAccountsRecId

LEFT JOIN dbo.SMRBIMainAccountCategoryStaging MACS
ON MAS.MainAccountCategory = MACS.MainAccountCategory

LEFT JOIN ETL.StringMap SM
ON SM.SourceSystem = 'D365FO'
	and SM.SourceTable = 'MainAccountStaging'
	and SM.SourceColumn = 'MainAccountType'
	and SM.Enum = MAS.MainAccountType


;
