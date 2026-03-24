/****** Object:  View [DWH].[V_DimWarehouseLocation]    Script Date: 03/03/2026 16:26:09 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimWarehouseLocation` AS


SELECT    RecId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(WarehouseLocationCode) AS WarehouseLocationCode
		, WarehouseCode
		, WarehouseName
		, WarehouseCodeName
		, WarehouseLocationType

FROM DataStore.WarehouseLocation

/* Create Unknown Member */

UNION All

SELECT	DISTINCT -1
		       , UPPER(CompanyId)
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
FROM dbo.SMRBIOfficeAddinLegalEntityStaging
;
