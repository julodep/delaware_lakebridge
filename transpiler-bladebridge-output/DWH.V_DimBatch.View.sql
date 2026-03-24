/****** Object:  View [DWH].[V_DimBatch]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimBatch` AS 


SELECT    RecId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(BatchCode) AS BatchCode 
		, UPPER(ProductCode) AS ProductCode
		, Description 
		, ExpiryDate
		, ProductionDate

FROM DataStore.Batch

/* Create unknown member */

UNION ALL

SELECT	DISTINCT -1
		, UPPER(C.CompanyCode)
		, '_N/A'
		, UPPER(P.ProductCode)
		, '_N/A'
		, '1900-01-01'
		, '1900-01-01'

FROM DataStore.Company C
INNER JOIN DataStore.Product P
ON C.CompanyCode = P.CompanyCode

UNION ALL

SELECT	DISTINCT -1
		, UPPER(CompanyCode)
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '1900-01-01'
		, '1900-01-01'

FROM DataStore.Company
;
