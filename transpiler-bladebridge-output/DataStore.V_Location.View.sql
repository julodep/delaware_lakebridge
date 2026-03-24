/****** Object:  View [DataStore].[V_Location]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_Location` AS 

--Distinct is added due to multiple exports per company
;
SELECT	DISTINCT DFTS.FinancialTagRecId AS LocationId
		, DFTS.`Value` AS LocationCode
		, DFTS.`Description` AS LocationName
		, DFTS.Value || ' ' || DFTS.DESCRIPTION AS LocationCodeName
		, DimensionName = DAS.DimensionName

FROM dbo.SMRBIDimensionFinancialTagStaging DFTS
INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory         

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE DAS.DimensionName = 'Location'
;
