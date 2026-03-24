/****** Object:  View [DataStore].[V_BudgetCode]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_BudgetCode` AS 


SELECT	DISTINCT UPPER(BCS.Company) AS CompanyCode
		       , BCS.BudgetRegisterRecId AS BudgetTransactionCode
		       , UPPER(BCS.BudgetCode) AS BudgetCodeName
		       , CASE WHEN LEN(BCS.`Description`) = 0 OR BCS.`Description` IS NULL 
				      THEN '_N/A' 
					  ELSE BCS.`Description` 
				 END AS BudgetCodeDescription

FROM dbo.SMRBIBudgetRegisterEntryHeaderStaging BCS
;
