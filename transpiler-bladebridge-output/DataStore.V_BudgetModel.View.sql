/****** Object:  View [DataStore].[V_BudgetModel]    Script Date: 03/03/2026 16:26:09 ******/








CREATE OR REPLACE VIEW `DataStore`.`V_BudgetModel` AS
 

SELECT	  UPPER(BMS.DataAreaId) AS CompanyCode
		, UPPER(BMS.BudgetModel) AS BudgetModelCode
		, CASE WHEN BMS.`Name` = '' THEN '_N/A' ELSE BMS.`Name` END AS BudgetModelName

FROM dbo.SMRBIBudgetModelStaging BMS
;
