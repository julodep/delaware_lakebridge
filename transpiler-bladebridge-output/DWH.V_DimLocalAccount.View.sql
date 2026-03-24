/****** Object:  View [DWH].[V_DimLocalAccount]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimLocalAccount` AS         


SELECT    LocalAccountId
		, UPPER(LocalAccountCode) AS LocalAccountCode
		, LocalAccountName
		, LocalAccountCodeName
		, DimensionName         

FROM DataStore.LocalAccount

/* Create Unknown Member */

UNION ALL 
        
SELECT	-1
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
;
