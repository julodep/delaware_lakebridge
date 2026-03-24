/****** Object:  View [DataStore].[V_Resource]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DataStore`.`V_Resource` AS


SELECT	  COALESCE(ORORS.ResourceId, '_N/A') AS ResourceCode
		, COALESCE(NULLIF(ORORS.ResourceName, ''), '_N/A') AS ResourceName
		, CAST(COALESCE(ORORS.ResourceId || ' - '  || NULLIF(ORORS.ResourceName, ''), '_N/A') AS STRING) AS ResourceCodeName
		, COALESCE(NULLIF(ORORGS.GroupId, ''), '_N/A') AS ResourceGroupCode
		, COALESCE(NULLIF(ORORGS.GroupName, ''), '_N/A') AS ResourceGroupName
		, CAST(COALESCE(NULLIF(ORORGS.GroupId, '') || ' - '  || NULLIF(ORORGS.GroupName, ''), '_N/A') AS STRING) AS ResourceGroupCodeName
		, COALESCE(ORORS.DataAreaId, '_N/A') AS CompanyCode
		, COALESCE(STRM.Name, '_N/A') AS ResourceType
		, COALESCE(NULLIF(ORORGS.InputWarehouseId, ''), '_N/A') AS InputWarehouseCode
		, COALESCE(NULLIF(ORORGS.InputWarehouseLocationId, ''), '_N/A') AS InputWarehouseLocationCode
		, COALESCE(NULLIF(ORORGS.OutputWarehouseId, ''), '_N/A') AS OutputWarehouseCode
		, COALESCE(NULLIF(ORORGS.OutputWarehouseLocationId, ''), '_N/A') AS OutputWarehouseLocationCode
		, COALESCE(ORORS.EfficiencyPercentage, 0) AS EfficiencyPercentage
		, COALESCE(NULLIF(ORORS.RouteGroupId, ''), '_N/A') AS RouteGroupCode
		, COALESCE(NULLIF(ORORS.HasFiniteSchedulingCapacity, ''), 0) AS HasFiniteSchedulingCapacity --Technical Field
		, COALESCE(RGA.ValidFrom, '1900-01-01') AS ValidFromDate
		, COALESCE(RGA.ValidTo, '1900-01-01') AS ValidToDate
		, COALESCE(ORORWCAS.WorkCalendarId, '_N/A') AS CalendarCode

FROM dbo.SMRBIOpResOperationsResourceStaging ORORS

LEFT JOIN dbo.SMRBIOpResOperationsResourceGroupAssignmentStaging RGA
ON ORORS.DataAreaId = RGA.DataAreaId
	and ORORS.ResourceId = RGA.OperationsResourceId

LEFT JOIN dbo.SMRBIOpResOperationsResourceGroupStaging ORORGS
ON ORORGS.DataAreaId = RGA.DataAreaId
	and ORORGS.GroupId = RGA.OperationsResourceGroupId

LEFT JOIN dbo.SMRBIOpResOperationsResourceWorkCalendarAssignmentStaging ORORWCAS
ON ORORWCAS.DataAreaId = ORORS.DataAreaId
	and ORORWCAS.OperationsResourceId = ORORS.ResourceId

LEFT JOIN ETL.StringMap STRM
ON STRM.SourceTable = 'OpResOperationsResourceStaging'
	and STRM.SourceColumn = 'OperationsResourceType'
	and STRM.Enum = CAST(ORORS.OperationsResourceType AS STRING)

WHERE ORORS.IsIndividualResource = 1 --Only individual resources

/* Add resource group as resource for production capacity planning */

UNION ALL

SELECT COALESCE(ORORGS.GroupId, '_N/A') AS ResourceCode
		, COALESCE(ORORGS.GroupName, '_N/A') AS ResourceName
		, CAST(COALESCE(ORORGS.GroupId || ' - '  || NULLIF(ORORGS.GroupName, ''), '_N/A') AS STRING) AS ResourceCodeName
		, COALESCE(ORORGS.GroupId, '_N/A') AS ResourceGroupCode
		, COALESCE(ORORGS.GroupName, '_N/A') AS ResourceGroupName
		, CAST(COALESCE(ORORGS.GroupId || ' - '  || NULLIF(ORORGS.GroupName, ''), '_N/A') AS STRING) AS ResourceGroupCodeName
		, ORORGS.DataAreaId AS CompanyCode
		, 'Resource Group' AS ResourceType
		, COALESCE(NULLIF(ORORGS.InputWarehouseId, ''), '_N/A') AS InputWarehouseCode
		, COALESCE(NULLIF(ORORGS.InputWarehouseLocationId, ''), '_N/A') AS InputWarehouseLocationCode
		, COALESCE(NULLIF(ORORGS.OutputWarehouseId, ''), '_N/A') AS OutputWarehouseCode
		, COALESCE(NULLIF(ORORGS.OutputWarehouseLocationId, ''), '_N/A') AS OutputWarehouseLocationCode
		, CAST(0 AS int) AS EfficiencyPercentage
		, N'_N/A' AS RouteGroupCode
		, CAST(0 AS int) AS HasFiniteSchedulingCapacity
		, CAST('1900-01-01' AS TIMESTAMP) AS ValidFromDate
		, CAST('9999-12-31' AS TIMESTAMP) AS ValidToDate
		, N'_N/A' AS CalendarCode

FROM dbo.SMRBIOpResOperationsResourceGroupStaging ORORGS
;
