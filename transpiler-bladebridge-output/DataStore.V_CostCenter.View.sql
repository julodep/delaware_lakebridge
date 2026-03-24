/****** Object:  View [DataStore].[V_CostCenter]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DataStore`.`V_CostCenter` AS 

/* Cost Center is a financial dimension --> Alter/Copy where required */
;
SELECT	DFTS.FinancialTagRecId AS CostCenterId
	  , DFTS.`Value` AS CostCenterCode
	  , DFTS.`Description` AS CostCenterName
	  , DFTS.Value || ' ' || DFTS.Description AS CostCenterCodeName
	  , DAS.DimensionName AS DimensionName

FROM dbo.SMRBIDImensionFinancialTagStaging DFTS

INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE 1=1
	and DAS.DimensionName = 'CostCenter'
;
