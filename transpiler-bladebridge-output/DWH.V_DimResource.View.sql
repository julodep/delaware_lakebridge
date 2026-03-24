/****** Object:  View [DWH].[V_DimResource]    Script Date: 03/03/2026 16:26:08 ******/








CREATE OR REPLACE VIEW `DWH`.`V_DimResource` AS 


SELECT	  UPPER(ResourceCode) AS ResourceCode
		, UPPER(CompanyCode) AS CompanyCode
		, ResourceName
		, ResourceCodeName
		, ResourceGroupCode
		, ResourceGroupName
		, ResourceGroupCodeName
		, ResourceType
		, InputWarehouseCode
		, InputWarehouseLocationCode
		, OutputWarehouseCode
		, OutputWarehouseLocationCode
		, EfficiencyPercentage
		, RouteGroupCode
		, HasFiniteSchedulingCapacity
		, CAST(ValidFromDate AS DATE) AS ValidFromDate
		, CAST(ValidToDate AS DATE) AS ValidToDate
		, CalendarCode AS CalendarCode

FROM DataStore.Resource

/* Create Unknown Member */

UNION ALL

SELECT	  '_N/A'
		, UPPER(CompanyCode)
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, -1
		, '_N/A'
		, 0
		, '1900-01-01'
		, '1900-01-01'
		, '_N/A'

FROM DataStore.Company
;
