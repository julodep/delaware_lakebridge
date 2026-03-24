/****** Object:  View [DataStore].[V_Route]    Script Date: 03/03/2026 16:26:08 ******/








CREATE OR REPLACE VIEW `DataStore`.`V_Route` AS


SELECT	DISTINCT COALESCE(RHS.RouteId, '_N/A') AS RouteCode --Distinct is added as there can be multiple costing operation resource IDs, but this information is not required
		       , COALESCE(NULLIF(RHS.RouteName, ''), '_N/A') AS RouteName
		       , CAST(COALESCE(RHS.RouteId || ' - '  || NULLIF(RHS.RouteName, ''), '_N/A') AS STRING) AS RouteCodeName
		       , COALESCE(NULLIF(RROS.OperationId, ''), '_N/A') AS OperationCode
		       , RANK() OVER (PARTITION BY RROS.RouteId, RROS.DataAreaId ORDER BY RROS.OperationNumber ASC) AS OperationSequence
		       , COALESCE(NULLIF(RROS.OperationNumber, ''), -1) AS OperationNumber
		       , COALESCE(NULLIF(RROS.NextRouteOperationNumber, ''), -1) AS OperationNumberNext
		       , COALESCE(RHS.DataAreaId, '_N/A') AS CompanyCode		       
		       , COALESCE(NULLIF(ROPS.RouteGroupId, ''), '_N/A') AS RouteGroupCode
		       , COALESCE(NULLIF(RGS.GroupName, ''), '_N/A') AS RouteGroupName
		       , CAST(COALESCE(ROPS.RouteGroupId || ' - '  || NULLIF(RGS.GroupName, ''), '_N/A') AS STRING) AS RouteGroupCodeName
		       , COALESCE(NULLIF(ROPS.ProductionSiteId, ''), '_N/A') AS SiteCode
		       , COALESCE(NULLIF(ISS.Name, ''), '_N/A') AS SiteName

		/* Additional information: add if required */
		--, ISNULL(NULLIF(ROPS.CostingOperationResourceId, ''), '_N/A') AS CostingOperationResourceId
		--, ISNULL(NULLIF(ROPS.ProductionSiteId, ''), '_N/A') AS ProductionSiteId
		--, ISNULL(ROPS.SetupTime, -1) AS SetupTime
		--, ISNULL(ROPS.ProcessTime, -1)	 AS ProcessTime
		--, ISNULL(NULLIF(ROPS.QuantityCostCategoryId, ''), '_N/A') AS QuantityCostCategoryId
		--, ISNULL(NULLIF(ROPS.SetupCostCategoryId, ''), '_N/A') AS SetupCostCategoryId
		--, ISNULL(NULLIF(ROPS.ProcessCostCategoryId, ''), '_N/A') AS ProcessCostCategoryId

FROM dbo.SMRBIRouteHeaderStaging RHS

LEFT JOIN dbo.SMRBIRouteRouteOperationStaging RROS
ON RHS.RouteId = RROS.RouteId
AND RHS.DataAreaId = RROS.DataAreaId

LEFT JOIN dbo.SMRBIRouteOperationPropertiesStaging ROPS
ON RROS.DataAreaId = ROPS.DataAreaId
AND RROS.RouteId = ROPS.RouteId
AND RROS.OperationId = ROPS.OperationId

LEFT JOIN dbo.SMRBIRouteGroupStaging RGS
ON RGS.DataAreaId = ROPS.DataAreaId
AND RGS.GroupId = ROPS.RouteGroupId

LEFT JOIN dbo.SMRBIInventSiteStaging ISS
ON ISS.DataAreaId = ROPS.DataAreaId
AND ISS.SiteId = ROPS.ProductionSiteId

WHERE 1=1
AND RROS.OperationPriority = 0 --Take only operations with the highest priority

/* Create unknown operation per Route */ 
/* Note! the reason for this is because the production order is linked to both a route and an operation, though some production order transactions have no operation ID (e.g. Indirect costs) */

UNION ALL

SELECT	DISTINCT COALESCE(RHS.RouteId, '_N/A') AS RouteCode
		       , COALESCE(NULLIF(RHS.RouteName, ''), '_N/A') AS RouteName
		       , CAST(COALESCE(RHS.RouteId || ' - '  || NULLIF(RHS.RouteName, ''), '_N/A') AS STRING) AS RouteCodeName
		       , '_N/A' AS OperationCode
		       , -1 AS OperationSequence
		       , -1 AS OperationNumber
		       , -1 AS OperationNumberNext
		       , COALESCE(RHS.DataAreaId, '_N/A') AS CompanyCode
		       , '_N/A' AS RouteGroupId
		       , '_N/A' AS RouteGroupName
		       , '_N/A' AS RouteGroupIdName
		       , '_N/A' AS SiteId
		       , '_N/A' AS SiteName

FROM dbo.SMRBIRouteHeaderStaging RHS
;
