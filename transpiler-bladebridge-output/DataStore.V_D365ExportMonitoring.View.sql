/****** Object:  View [DataStore].[V_D365ExportMonitoring]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_D365ExportMonitoring` as

/****************************************************************************************************************************************

Purpose: This view is intended to give an overview of the performance of the export jobs between D365 and the Azure SQL database 

The following parameters need to be filled in:
	- CompanyId: Include the company ID in the job export name, to facilitate the export 
	- Job ID: Determine a specific naming convention for the job export name, such that it can be easily filtered (e.g. BI_DailyExport...)
	- Definition Group ID: Determine a specific naming convention for the job export name, such that it can be easily filtered (e.g. EXPORTSTATUS)
	- Source Format: this is the name of the Azure SQL database connection in D365

****************************************************************************************************************************************/
;
SELECT	  S.DefinitionGroupId
        , S.JobId
		, CompanyId = '_N/A' --See comment above!
		, S.EntityName
		--, S.StagingStartDateTime --Not relavant => we skip staging most of the time
		--, S.StagingEndDateTime
		, S.TargetStartDateTime
		, S.TargetEndDateTime
		, ExportDate = CAST(DATEADD(HOUR, 4, S.TargetEndDateTime) AS Date) --dateadd because some exports start already at 22h => should be grouped together with exports after 00h
		, S.SequenceNumber
		, ExportingEntityInSeconds = DATEDIFF_BIG(SECOND, S.TargetStartDateTime, S.TargetEndDateTime)
		, ExportingEntityInMinutes = CAST(ROUND(DATEDIFF_BIG(SECOND, S.TargetStartDateTime, S.TargetEndDateTime)/60,0) AS Int)
		, AverageExportSeconds = COALESCE(AVGEXP.AverageExportSeconds, 0) --Possible to be NULL => if export failed for the last month for a specific company
		, AverageExportMinutes = COALESCE(CAST(ROUND(AVGEXP.AverageExportSeconds/60,0) AS Int), 0)
		, S.StagingRecordsToBeProcessedCount
		, S.StagingRecordsCreatedCount
		, S.TargetRecordsCreatedCount -- In case of full push or incremental (the new lines)
		, S.TargetRecordsUpdatedCount -- In case of incremental, the changed lines
		, StagingStatus = CAST(S.StagingStatus AS STRING)
		, TargetStatus = CAST(S.TargetStatus AS STRING)
		, S.FailLevelOnError
		, IsFailed = CAST(Case when TargetStatus = 4 THEN 0 ELSE 1 END AS BOOLEAN)
		, IsLongerRunTime = CAST(CASE
WHEN DATEDIFF_BIG(SECOND, S.TargetStartDateTime, S.TargetEndDateTime) > (AVGEXP.AverageExportSeconds * 1.1) /*--If exports takes 10% longer then the average*/
AND  DATEDIFF_BIG(SECOND, S.TargetStartDateTime, S.TargetEndDateTime) - AVGEXP.AverageExportSeconds > 20	--But the difference is also  bigger then 20s, to avoid an overload of little entities
THEN 1
ELSE 0 END AS BOOLEAN)
FROM dbo.DataManagementExecutionJobDetailStaging S 

LEFT JOIN ( 
	-- Average is calculated based on successful exports during the last 2 month
	SELECT 
		DD.DefinitionGroupId
		, DD.EntityName
		, AverageExportSeconds = COALESCE(AVG( CAST(DD.ExportingEntityInSeconds AS decimal(25,8)) ), 0)
	FROM (
		SELECT 
			DefinitionGroupId
			, EntityName
			, TargetStartDateTime, TargetEndDateTime
			, DATEDIFF_BIG(SECOND, TargetStartDateTime, TargetEndDateTime) AS ExportingEntityInSeconds
		FROM dbo.DataManagementExecutionJobDetailStaging
		WHERE 1=1
			AND DefinitionGroupId NOT LIKE '%EXPORTSTATUS%' --See comment above!
			AND JobId LIKE '%BI_Daily%' --See comment above! 
			AND CAST(StagingEndDateTime AS Date) > DATEADD(MONTH, -2, CAST(current_timestamp() AS date)) --Do not filter on last 2 weeks, but 2 months (not all time, because longer export times are expected and normal)
			AND TargetStatus = 4
		) DD 
	GROUP BY DefinitionGroupId, EntityName
	) AVGEXP

ON S.DefinitionGroupId = AVGEXP.DefinitionGroupId
	AND S.EntityName = AVGEXP.EntityName

WHERE 1=1
	 AND CAST(S.StagingEndDateTime AS Date) > DATEADD(WEEK, -4, CAST(current_timestamp() AS date))
	 AND S.JobId LIKE '%BI_Daily%' --See comment above!
	 AND S.DefinitionGroupId NOT LIKE '%EXPORTSTATUS%' --See comment above!
	 AND SourceFormat = 'SMART_BI' --See comment above!
;
