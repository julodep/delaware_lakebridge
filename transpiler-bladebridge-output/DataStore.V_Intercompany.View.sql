/****** Object:  View [DataStore].[V_Intercompany]    Script Date: 03/03/2026 16:26:08 ******/








CREATE OR REPLACE VIEW `DataStore`.`V_Intercompany` AS 

--Distinct is added due to multiple exports per company
;
SELECT	DISTINCT DFTS.FinancialTagRecId AS IntercompanyId
		, DFTS.`Value` AS IntercompanyCode
		, DFTS.`Description` AS IntercompanyName
		, DFTS.Value || ' ' || DFTS.DESCRIPTION AS IntercompanyCodeName
		, DimensionName = DAS.DimensionName

FROM dbo.SMRBIDimensionFinancialTagStaging DFTS
INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory         

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE DAS.DimensionName = 'Intercompany'
;
