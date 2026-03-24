/****** Object:  View [DataStore].[V_CaseActivity]    Script Date: 03/03/2026 16:26:08 ******/





CREATE OR REPLACE VIEW `DataStore`.`V_CaseActivity` AS


SELECT COALESCE(NULLIF(CaseDetailBase.CaseId,''), '_N/A') AS CaseCode
	 , COALESCE(Activities.ActivityNumber, '_N/A') AS ActivityNumber
       /*Dates*/
	 , CAST(COALESCE(Activities.StartDateTime, '1900-01-01') AS DATE) AS StartDateTime
	 , CAST(COALESCE(Activities.EndDateTime, '1900-01-01') AS DATE) AS EndDateTime
	 , CAST(COALESCE(Activities.ActualEndDateTime, '1900-01-01') AS DATE) AS ActualEndDateTime	   
       /* Case Activity Details */
	 , Activities.DataAreaId AS CompanyCode
	 , CAST(COALESCE(NULLIF(StringMapsmmActivitysmmShowTimeAs.Name,''), '_N/A') AS STRING) AS ActivityTimeType
	 , CAST(COALESCE(NULLIF(StringMapsmmActivityTaskTimeType.Name,''), '_N/A') AS STRING) AS ActivityTaskTimeType
	 , COALESCE(Activities.ActualWork, 0) AS ActualWork
	 , CAST(COALESCE(NULLIF(StringMapAllDay.Name,''), '_N/A') AS STRING) AS AllDay
	 , CAST(COALESCE(NULLIF(StringMapsmmActivityCategory.Name,''), '_N/A') AS STRING) AS Category
	 , CAST(COALESCE(NULLIF(StringMapClosed.Name,''), '_N/A') AS STRING) AS Closed
	 , COALESCE(NULLIF(HCMDoneByWorker.Name, ''), '_N/A') AS DoneByWorker
	 , COALESCE(Activities.PercentageCompleted, 0) AS PercentageCompleted
	 , COALESCE(NULLIF(Activities.Purpose,''), '_N/A') AS Purpose
	 , COALESCE(NULLIF(HCMResponsibleWorker.Name, ''), '_N/A') AS ResponsibleWorker
	 , CAST(COALESCE(NULLIF(StringMapsmmActivityStatus.Name,''), '_N/A') AS STRING) AS Status
	 , COALESCE(NULLIF(Activities.TypeId,''), '_N/A') AS TypeCode
	 , COALESCE(NULLIF(Activities.UserMemo,''), '_N/A') AS UserMemo

FROM dbo.SMRBICaseDetailBaseStaging AS CaseDetailBase

LEFT JOIN dbo.SMRBIsmmActivityParentLinkTableStaging AS ActivityParentLinkTable

ON CaseDetailBase.CaseDetailBaseRecId = ActivityParentLinkTable.RefRecId
AND CaseDetailBase.DataAreaId = ActivityParentLinkTable.DataAreaId

LEFT JOIN dbo.SMRBIsmmActivitiesStaging AS Activities
ON ActivityParentLinkTable.ActivityNumber = Activities.ActivityNumber

LEFT JOIN dbo.SMRBIHcmWorkerStaging AS HCMDoneByWorker
ON HCMDoneByWorker.PersonnelNumber = Activities.DoneByWorker_PersonnelNumber

LEFT JOIN dbo.SMRBIHcmWorkerStaging AS HCMResponsibleWorker
ON HCMResponsibleWorker.PersonnelNumber = Activities.DoneByWorker_PersonnelNumber

--/*To get enum labels*/
LEFT JOIN ETL.StringMap StringMapsmmActivityTaskTimeType
ON StringMapsmmActivityTaskTimeType.SourceTable = 'smmActivityTaskTimeType'

AND StringMapsmmActivityTaskTimeType.Enum = CAST(Activities.ActivityTimeType AS STRING)
LEFT JOIN ETL.StringMap StringMapsmmActivitysmmShowTimeAs

ON StringMapsmmActivitysmmShowTimeAs.SourceTable = 'StringMapsmmActivitysmmShowTimeAs'
AND StringMapsmmActivitysmmShowTimeAs.Enum = CAST(Activities.ActivityTimeType AS STRING)

LEFT JOIN ETL.StringMap StringMapAllDay
ON StringMapAllDay.SourceTable = 'NoYes'
AND StringMapAllDay.Enum = CAST(Activities.AllDay AS STRING)

LEFT JOIN ETL.StringMap StringMapsmmActivityCategory
ON StringMapsmmActivityCategory.SourceTable = 'smmActivityCategory'
AND StringMapsmmActivityCategory.Enum = CAST(Activities.Category AS STRING)

LEFT JOIN ETL.StringMap StringMapClosed
ON StringMapClosed.SourceTable = 'NoYes'
AND StringMapClosed.Enum = CAST(Activities.Closed AS STRING)

LEFT JOIN ETL.StringMap StringMapIsMasterAppointment
ON StringMapIsMasterAppointment.SourceTable = 'NoYes'
AND StringMapIsMasterAppointment.Enum = CAST(Activities.IsMasterAppointment AS STRING)

LEFT JOIN ETL.StringMap StringMapIsTemplate
ON StringMapIsTemplate.SourceTable = 'NoYes'
AND StringMapIsTemplate.Enum = CAST(Activities.IsTemplate AS STRING)

LEFT JOIN ETL.StringMap StringMapKeepSynchronized
ON StringMapKeepSynchronized.SourceTable = 'NoYes'
AND StringMapKeepSynchronized.Enum = CAST(Activities.KeepSynchronized AS STRING)

LEFT JOIN ETL.StringMap StringMapModified
ON StringMapModified.SourceTable = 'NoYes'
AND StringMapModified.Enum = CAST(Activities.Modified AS STRING)

LEFT JOIN ETL.StringMap StringMapRecurrenceState
ON StringMapRecurrenceState.SourceTable = 'NoYes'
AND StringMapRecurrenceState.Enum = CAST(Activities.RecurrenceState AS STRING)
	
LEFT JOIN ETL.StringMap StringMapReminderActive
ON StringMapReminderActive.SourceTable = 'NoYes'
AND StringMapReminderActive.Enum = CAST(Activities.ReminderActive AS STRING)	

LEFT JOIN ETL.StringMap StringMapResponseRequested
ON StringMapResponseRequested.SourceTable = 'NoYes'
AND StringMapResponseRequested.Enum = CAST(Activities.ResponseRequested AS STRING)
	
LEFT JOIN ETL.StringMap StringMapsmmSensitivity
ON StringMapsmmSensitivity.SourceTable = 'smmSensitivity'
AND StringMapsmmSensitivity.Enum = CAST(Activities.Sensitivity AS STRING)	

LEFT JOIN ETL.StringMap StringMapsmmActivityStatus
ON StringMapsmmActivityStatus.SourceTable = 'smmActivityStatus'
AND StringMapsmmActivityStatus.Enum = CAST(Activities.Status AS STRING)

LEFT JOIN ETL.StringMap StringMapsmmActivityPriority
ON StringMapsmmActivityPriority.SourceTable = 'smmActivityPriority'
AND StringMapsmmActivityPriority.Enum = CAST(Activities.TaskPriority AS STRING)	

LEFT JOIN ETL.StringMap StringMapsmmTeamTask
ON StringMapsmmTeamTask.SourceTable = 'NoYes'
AND StringMapsmmTeamTask.Enum = CAST(Activities.TeamTask AS STRING)	

/*Select only Cases linked to an Activity*/
WHERE Activities.ActivityNumber <> ''
;
