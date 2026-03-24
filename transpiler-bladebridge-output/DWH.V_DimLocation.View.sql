/****** Object:  View [DWH].[V_DimLocation]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimLocation` AS         


SELECT    LocationId
		, UPPER(LocationCode) AS LocationCode
		, LocationName
		, LocationCodeName
		, DimensionName         

FROM DataStore.Location

/* Create Unknown Member */

UNION ALL 
        
SELECT	-1
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
;
