/****** Object:  View [DataStore].[V_ShipmentContract]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_ShipmentContract` AS 

--Distinct is added due to multiple exports per company
;
SELECT	DISTINCT DFTS.FinancialTagRecId AS ShipmentContractId
		, DFTS.`Value` AS ShipmentContractCode
		, DFTS.`Description` AS ShipmentContractName
		, DFTS.Value || ' ' || DFTS.DESCRIPTION AS ShipmentContractCodeName
		, DimensionName = DAS.DimensionName

FROM dbo.SMRBIDimensionFinancialTagStaging DFTS
INNER JOIN dbo.SMRBIDimensionAttributeDirCategoryStaging DADCS
ON DFTS.FinancialTagCategory = DADCS.DirCategory         

INNER JOIN dbo.SMRBIDimensionAttributeStaging DAS
ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE DAS.DimensionName = 'Shipment_Contract'
;
