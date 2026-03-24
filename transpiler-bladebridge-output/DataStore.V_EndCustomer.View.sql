/****** Object:  View [DataStore].[V_EndCustomer]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_EndCustomer` AS 

--Distinct is added due to multiple exports per company
;
SELECT	DISTINCT DFTS.FinancialTagRecId AS EndCustomerId
		, DFTS.`Value` AS EndCustomerCode
		, DFTS.`Description` AS EndCustomerName
		, DFTS.Value || ' ' || DFTS.DESCRIPTION AS EndCustomerCodeName
		, DimensionName = DAS.DimensionName

FROM dbo.SMRBIDimensionFinancialTagStaging DFTS
INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory         

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE DAS.DimensionName = 'Endcustomer'
;
