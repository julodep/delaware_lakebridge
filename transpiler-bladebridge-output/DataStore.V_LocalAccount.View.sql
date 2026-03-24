/****** Object:  View [DataStore].[V_LocalAccount]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_LocalAccount` AS 

--Distinct is added due to multiple exports per company
;
SELECT	DISTINCT DFTS.FinancialTagRecId AS LocalAccountId
		, DFTS.`Value` AS LocalAccountCode
		, DFTS.`Description` AS LocalAccountName
		, DFTS.Value || ' ' || DFTS.DESCRIPTION AS LocalAccountCodeName
		, DimensionName = DAS.DimensionName

FROM dbo.SMRBIDimensionFinancialTagStaging DFTS
INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory         

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE DAS.DimensionName = 'Local_Account'
;
