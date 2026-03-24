/****** Object:  View [DataStore].[V_Department]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_Department` AS 

--Distinct is added due to multiple exports per company
;
SELECT	DISTINCT DFTS.FinancialTagRecId AS DepartmentId
		, DFTS.`Value` AS DepartmentCode
		, DFTS.`Description` AS DepartmentName
		, DFTS.Value || ' ' || DFTS.DESCRIPTION AS DepartmentCodeName
		, DimensionName = DAS.DimensionName

FROM dbo.SMRBIDimensionFinancialTagStaging DFTS
INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory         

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE DAS.DimensionName = 'Department'
;
