/****** Object:  View [DataStore].[V_Operations]    Script Date: 03/03/2026 16:26:09 ******/








CREATE OR REPLACE VIEW `DataStore`.`V_Operations` AS


SELECT	DISTINCT COALESCE(NULLIF(RROS.OperationId, ''), '_N/A') AS OperationCode
		, COALESCE(NULLIF(OS.OperationName, ''), '_N/A') AS OperationName
		, RANK() OVER (PARTITION BY RROS.RouteId, RROS.DataAreaId ORDER BY RROS.OperationNumber ASC) AS OperationSequence
		, COALESCE(CAST(NULLIF(RROS.OperationNumber, '') AS STRING), -1) AS OperationNumber
		, COALESCE(NULLIF(RROS.NextRouteOperationNumber, ''), -1) AS OperationNumberNext
		, COALESCE(RROS.DataAreaId, '_N/A') AS CompanyCode
		, COALESCE(NULLIF(OperationPriority, ''), 0) AS OperationPriority

FROM dbo.SMRBIRouteRouteOperationStaging RROS

LEFT JOIN dbo.SMRBIRouteOperationStaging OS
ON RROS.OperationId = OS.OperationId
	AND RROS.DataAreaId = OS.DataAreaId

WHERE 1=1
;
