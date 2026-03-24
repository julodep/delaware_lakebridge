/****** Object:  View [DWH].[V_DimProductFD]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimProductFD` AS         


SELECT    ProductFDId
		, UPPER(ProductFDCode) AS ProductFDCode
		, ProductFDName
		, ProductFDCodeName
		, DimensionName         

FROM DataStore.ProductFD

/* Create Unknown Member */

UNION ALL 
        
SELECT	-1
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
;
