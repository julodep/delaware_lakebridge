/****** Object:  View [DWH].[V_DimRoute]    Script Date: 03/03/2026 16:26:09 ******/











CREATE OR REPLACE VIEW `DWH`.`V_DimRoute` AS 


SELECT	  UPPER(RouteCode) AS RouteCode
		, UPPER(CompanyCode) AS CompanyCode
		, RouteName
		, UPPER(RouteCodeName) AS RouteCodeName
		, UPPER(OperationCode) AS OperationCode
		, OperationSequence
		, OperationNumber
		, OperationNumberNext
		, UPPER(RouteGroupCode) AS RouteGroupCode
		, RouteGroupName
		, RouteGroupCodeName AS RouteGroupCodeName
		, UPPER(SiteCode) AS SiteCode
		, SiteName

FROM DataStore.Route

/* Create default unknown members */

UNION ALL

SELECT	DISTINCT '_N/A'
			   , UPPER(CompanyCode)
			   , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , -1
		       , -1
		       , -1
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'

FROM DataStore.Company

UNION ALL

SELECT	'_N/A'
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
	  , -1
	  , -1
	  , -1
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
;
