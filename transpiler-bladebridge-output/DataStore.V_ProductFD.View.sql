/****** Object:  View [DataStore].[V_ProductFD]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_ProductFD` AS 

--Distinct is added due to multiple exports per company
;
SELECT	DISTINCT DFTS.FinancialTagRecId AS ProductFDId
		, DFTS.`Value` AS ProductFDCode
		, DFTS.`Description` AS ProductFDName
		, DFTS.Value || ' ' || DFTS.DESCRIPTION AS ProductFDCodeName
		, DimensionName = DAS.DimensionName

FROM dbo.SMRBIDimensionFinancialTagStaging DFTS
INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory         

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE DAS.DimensionName = 'Product'
;
