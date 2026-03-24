/****** Object:  View [DWH].[V_DimIntercompany]    Script Date: 03/03/2026 16:26:08 ******/








CREATE OR REPLACE VIEW `DWH`.`V_DimIntercompany` AS         


SELECT    IntercompanyId
		, UPPER(IntercompanyCode) AS IntercompanyCode
		, IntercompanyName
		, IntercompanyCodeName
		, DimensionName         

FROM DataStore.Intercompany

/* Create Unknown Member */

UNION ALL 
        
SELECT	-1
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
;
