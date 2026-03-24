/****** Object:  View [DWH].[V_DimOperations]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimOperations` AS


SELECT    UPPER(OperationCode) AS OperationCode
		, UPPER(CompanyCode) AS CompanyCode
		, OperationName
		, OperationNumber
		, OperationNumberNext
		, OperationPriority
		, OperationSequence

FROM DataStore.Operations

/* Create Unknown Member */

UNION ALL

SELECT	DISTINCT '_N/A'
		, UPPER(CompanyCode)
		, '_N/A'
		, -1
		, -1
		, -1
		, -1
FROM DataStore.Company

UNION ALL

SELECT	'_N/A'
		, '_N/A'
		, '_N/A'
		, -1
		, -1
		, -1
		, -1
;
