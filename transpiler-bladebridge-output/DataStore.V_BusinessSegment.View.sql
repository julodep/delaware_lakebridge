/****** Object:  View [DataStore].[V_BusinessSegment]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_BusinessSegment` AS 

--Distinct is added due to multiple exports per company
;
SELECT	DISTINCT DFTS.FinancialTagRecId AS BusinessSegmentId
		, DFTS.`Value` AS BusinessSegmentCode
		, DFTS.`Description` AS BusinessSegmentName
		, DFTS.Value || ' ' || DFTS.DESCRIPTION AS BusinessSegmentCodeName
		, DimensionName = DAS.DimensionName

FROM dbo.SMRBIDimensionFinancialTagStaging DFTS
INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory         

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE DAS.DimensionName = 'Business_segment'
;
