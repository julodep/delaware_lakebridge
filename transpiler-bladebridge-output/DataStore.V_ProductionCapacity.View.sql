/****** Object:  View [DataStore].[V_ProductionCapacity]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_ProductionCapacity` AS

/* Note! Available capacity will be determined in the cube */
;
WITH ResourceCalendarMapping AS (
	SELECT    DataAreaId
			, OperationsResourceId
			, ResourceType = 'Single'
			, ValidFrom
			, ValidTo
			, WorkCalendarId
	FROM dbo.SMRBIOpResOperationsResourceWorkCalendarAssignmentStaging

	UNION ALL 

	SELECT DataAreaId
			, OperationsResourceId = OperationsResourceGroupId
			, ResourceType = 'Group'
			, ValidFrom
			, ValidTo
			, WorkCalendarId
	FROM dbo.SMRBIOpResOperationsResourceGroupWorkCalendarAssignmentStaging)

--Step 1: Determine the maximum capacity per resource and plan version
SELECT    CONCAT(RCM.DataAreaId,WCTIS.WorkCalendarDate,RCM.WorkCalendarId,RCM.OperationsResourceId) AS ProductionCapacityIdScreening
		, RCM.DataAreaId AS CompanyCode
		, COALESCE(RPVS.ReqPlanId, '_N/A') AS PlanVersion
		, COALESCE(WCTIS.WorkCalendarDate, '1900-01-01') AS CapacityDate
		, COALESCE(RCM.WorkCalendarId, '_N/A') AS CalendarCode
		, COALESCE(RCM.OperationsResourceId, '_N/A') AS ResourceCode
		, 'Max. Capacity' AS RefType
		, '_N/A' AS RefCode
		
		/* Measures */
		, CASE WHEN ResourceType = 'Group' 
			   THEN 0 
			   ELSE COALESCE(ROUND(WCTIS.MaximumCapacity, 3), 0) 
		  END AS MaximumCapacity --In hours
		, CAST(0 AS DECIMAL(38,17)) AS ReservedCapacity
		, CAST(0 AS DECIMAL(38,17)) AS AvailableCapacity

FROM ResourceCalendarMapping RCM

INNER JOIN dbo.SMRBIReqPlanVersionStaging RPVS
ON RCM.DataAreaId = RPVS.ReqPlanDataAreaId

LEFT JOIN 
	(SELECT	DISTINCT DataAreaId = WCD.DataAreaId
			, WorkCalendarDate = WCD.CalendarDate
			, WorkCalendarId = WCD.WorkCalendarId
			, MaximumCapacity = COALESCE(WCTIS.MaximumCapacity, WCTIS2.MaximumCapacity, 0)
		FROM 

			--Step 1: Select calendar dates per calendar
			(SELECT DISTINCT WorkCalendarId = WorkCalendarId COLLATE DATABASE_DEFAULT
								, CalendarDate = CalendarDate
								, DataAreaId = DataAreaId COLLATE DATABASE_DEFAULT
				FROM dbo.SMRBIWorkCalendarDayStaging) WCD

				--Step 2: Join to retrieve max capacity by calendar
				LEFT JOIN 
					(SELECT DataAreaId
							, WorkCalendarDate
							, WorkCalendarId
							, MaximumCapacity = SUM(((EndTime - StartTime) / 3600.0) * EfficiencyPercentage/100) --In hours
						FROM dbo.SMRBIWorkCalendarTimeIntervalStaging --WHERE WORKCALENDARID like 'R-M-39%' and WORKCALENDARDATE BETWEEN '2018-11-19' and '2018-11-25'
						GROUP BY DataAreaId, WorkCalendarDate, WorkCalendarId) WCTIS
				ON WCD.DataAreaId = WCTIS.DataAreaId
					and WCD.WorkCalendarId = WCTIS.WorkCalendarId
					and WCD.CalendarDate = WCTIS.WorkCalendarDate

				--Step 3: Check if for a certain calendar, a base calendar exists
				LEFT JOIN 
					(SELECT DISTINCT BaseCalendarId = COALESCE(NULLIF(BasicCalendarId, ''), '_N/A')
							, CalendarId
							, DataAreaId 
						FROM dbo.SMRBIWorkCalendarStaging
						) BCM
				ON WCD.DataAreaId = BCM.DataAreaId
					and WCD.WorkCalendarId = BCM.CalendarId

				--Step 4: Join to retrieve max capacity by BASE calendar (only in the case when there is no max capacity for the detailed calendar)
				LEFT JOIN 
					(SELECT DataAreaId = DataAreaId COLLATE DATABASE_DEFAULT
							, WorkCalendarDate = WorkCalendarDate
							, WorkCalendarId = WorkCalendarId COLLATE DATABASE_DEFAULT
							, MaximumCapacity = SUM(((EndTime - StartTime) / 3600.0) * EfficiencyPercentage/100) --In hours
						FROM dbo.SMRBIWorkCalendarTimeIntervalStaging --WHERE WORKCALENDARID = 'CAL3X8' and WORKCALENDARDATE BETWEEN '2018-11-19' and '2018-11-24'
						GROUP BY DataAreaId, WorkCalendarDate, WorkCalendarId) WCTIS2
				ON WCD.DataAreaId = WCTIS2.DataAreaId
					and WCD.CalendarDate = WCTIS2.WorkCalendarDate
					and BCM.BaseCalendarId = WCTIS2.WorkCalendarId

	) WCTIS --Aggregation is done at this level since it is possible to have multiple working times per work calendar date
ON RCM.DataAreaId = WCTIS.DataAreaId
	and RCM.WorkCalendarId = WCTIS.WorkCalendarId
	and WCTIS.WorkCalendarDate BETWEEN RCM.ValidFrom and RCM.ValidTo

UNION ALL

--Step 2: Determine the actual reserved capacity
SELECT	ProductionCapacityIdScreening = CONCAT(WCCRS.DataAreaId,WCCRS.TransDate,WCCRS.WorkCalendarId,WCCRS.WrkCtrId)
		, CompanyId = WCCRS.DataAreaId
		, PlanVersion = COALESCE(WCCRS.ReqPlanId, '_N/A')
		, CapacityDate = COALESCE(WCCRS.TransDate, '1900-01-01')
		, CalendarId = COALESCE(WCCRS.WorkCalendarId, '_N/A')
		, ResourceId = COALESCE(WCCRS.WrkCtrId, '_N/A')
		, RefType = COALESCE(WCCRS.RefType, '_N/A')
		, RefId = COALESCE(WCCRS.RefId, '_N/A')
		
		/* Measures */
		, MaximumCapacity = CAST(0 AS DECIMAL(38,17)) --ISNULL(ROUND(WCTIS.MaximumCapacity * WCCRS.WrkCtrLoadPct/100, 3), 0) --In hours
		, ReservedCapacity = COALESCE(WCCRS.ReservedCapacity, 0)
		, AvailableCapacity = CAST(0 AS DECIMAL(38,17)) --ISNULL(ROUND(WCTIS.MaximumCapacity * WCCRS.WrkCtrLoadPct/100, 3), 0) - ISNULL(WCCRS.ReservedCapacity, 0) --In hours

FROM 
	(SELECT	WCCRS.DataAreaId
			, RPVS.ReqPlanId
			, RefType = SM.Name
			, WCCRS.RefId
			, WCCRS.TransDate
			, WCCRS.WrkCtrId
			, WCCRS.WrkCtrLoadPct
			, ReservedCapacity = COALESCE(SUM(ROUND(WCCRS.WrkCtrSec / 3600.0, 3)), 0) --In hours
			, WorkCalendarId = COALESCE(ORORWCAS.WorkCalendarId, ORORGWCAS.WorkCalendarId)

		FROM dbo.SMRBIWrkCtrCapResStaging WCCRS

		LEFT JOIN dbo.SMRBIReqPlanVersionStaging RPVS
		ON WCCRS.PlanVersion = RPVS.ReqPlanVersionRecId
			and WCCRS.DataAreaId = RPVS.ReqPlanDataAreaId

		LEFT JOIN ETL.StringMap SM
		ON SM.SourceTable = 'SMRBIWrkCtrCapResStaging'
			and SM.SourceColumn = 'RefType'
			and SM.Enum = WCCRS.RefType

		LEFT JOIN dbo.SMRBIOpResOperationsResourceWorkCalendarAssignmentStaging ORORWCAS
		ON WCCRS.DataAreaId = ORORWCAS.DataAreaId
			and WCCRS.WrkCtrId = ORORWCAS.OperationsResourceId
			and current_timestamp() BETWEEN ORORWCAS.ValidFrom and ORORWCAS.ValidTo

		LEFT JOIN dbo.SMRBIOpResOperationsResourceGroupWorkCalendarAssignmentStaging ORORGWCAS
		ON WCCRS.DataAreaId = ORORGWCAS.DataAreaId
			and WCCRS.WrkCtrId = ORORGWCAS.OperationsResourceGroupId
			and current_timestamp() BETWEEN ORORGWCAS.ValidFrom and ORORGWCAS.ValidTo

		WHERE 1=1

		GROUP BY WCCRS.DataAreaId
			, RPVS.ReqPlanId
			, WCCRS.RefId
			, SM.Name
			, WCCRS.TransDate
			, WCCRS.WrkCtrId
			, WCCRS.WrkCtrLoadPct
			, COALESCE(ORORWCAS.WorkCalendarId, ORORGWCAS.WorkCalendarId)
	) WCCRS
;
