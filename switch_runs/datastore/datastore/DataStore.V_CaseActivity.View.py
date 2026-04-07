# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_CaseActivity.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_CaseActivity.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Translate the T‑SQL CREATE VIEW into a Unity‑Catalog view
# ------------------------------------------------------------------
# • All table & column references are fully‑qualified using the
#   `dbe_dbx_internships` and `datastore` placeholders defined at the top of
#   this notebook.
# • T‑SQL functions are replaced with their Spark SQL equivalents.
# ------------------------------------------------------------------
cat = "dbe_dbx_internships"      # replace with your catalog name
sch = "datastore"       # replace with your schema name

# COMMAND ----------

# Build the SQL string for the view
view_sql = f"""
CREATE OR REPLACE VIEW {cat}.{sch}.V_CaseActivity AS
SELECT
    -- 1️⃣  Case code (empty → '_N/A')
    coalesce(nullif(CaseDetailBase.CaseId, ''), '_N/A')            AS CaseCode,

    -- 2️⃣  The activity number (empty → '_N/A')
    coalesce(Activities.ActivityNumber, '_N/A')                    AS ActivityNumber,

    -- 3️⃣  Dates – force NULL → 1900‑01‑01 and cast to DATE
    coalesce(cast(Activities.StartDateTime   AS date), cast('1900-01-01' AS date)) AS StartDateTime,
    coalesce(cast(Activities.EndDateTime     AS date), cast('1900-01-01' AS date)) AS EndDateTime,
    coalesce(cast(Activities.ActualEndDateTime AS date), cast('1900-01-01' AS date)) AS ActualEndDateTime,

    Activities.DataAreaId                                            AS CompanyCode,

    -- 4️⃣  Look‑ups from the StringMap tables (empty → '_N/A')
    coalesce(nullif(StringMapsmmActivitysmmShowTimeAs.Name, ''), '_N/A') AS ActivityTimeType,
    coalesce(nullif(StringMapsmmActivityTaskTimeType.Name, ''), '_N/A')   AS ActivityTaskTimeType,

    -- 5️⃣  Numbers – default 0 when NULL
    coalesce(Activities.ActualWork, 0)                                 AS ActualWork,
    coalesce(nullif(StringMapAllDay.Name, ''), '_N/A')                AS AllDay,
    coalesce(nullif(StringMapsmmActivityCategory.Name, ''), '_N/A')   AS Category,
    coalesce(nullif(StringMapClosed.Name, ''), '_N/A')                AS Closed,
    coalesce(nullif(HCMDoneByWorker.Name, ''), '_N/A')                AS DoneByWorker,
    coalesce(Activities.PercentageCompleted, 0)                       AS PercentageCompleted,
    coalesce(nullif(Activities.Purpose, ''), '_N/A')                  AS Purpose,
    coalesce(nullif(HCMResponsibleWorker.Name, ''), '_N/A')           AS ResponsibleWorker,
    coalesce(nullif(StringMapsmmActivityStatus.Name, ''), '_N/A')     AS Status,
    coalesce(nullif(Activities.TypeId, ''), '_N/A')                   AS TypeCode,
    coalesce(nullif(Activities.UserMemo, ''), '_N/A')                AS UserMemo

FROM {cat}.{sch}.SMRBICaseDetailBaseStaging   AS CaseDetailBase
LEFT JOIN {cat}.{sch}.SMRBIsmmActivityParentLinkTableStaging  AS ActivityParentLinkTable
    ON CaseDetailBase.CaseDetailBaseRecId = ActivityParentLinkTable.RefRecId
   AND CaseDetailBase.DataAreaId          = ActivityParentLinkTable.DataAreaId

LEFT JOIN {cat}.{sch}.SMRBIsmmActivitiesStaging  AS Activities
    ON ActivityParentLinkTable.ActivityNumber = Activities.ActivityNumber

LEFT JOIN {cat}.{sch}.SMRBIHcmWorkerStaging      AS HCMDoneByWorker
    ON HCMDoneByWorker.PersonnelNumber = Activities.DoneByWorker_PersonnelNumber

LEFT JOIN {cat}.{sch}.SMRBIHcmWorkerStaging      AS HCMResponsibleWorker
    ON HCMResponsibleWorker.PersonnelNumber = Activities.DoneByWorker_PersonnelNumber

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapsmmActivityTaskTimeType
    ON StringMapsmmActivityTaskTimeType.SourceTable = 'smmActivityTaskTimeType'
   AND StringMapsmmActivityTaskTimeType.Enum = cast(Activities.ActivityTimeType as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapsmmActivitysmmShowTimeAs
    ON StringMapsmmActivitysmmShowTimeAs.SourceTable = 'StringMapsmmActivitysmmShowTimeAs'
   AND StringMapsmmActivitysmmShowTimeAs.Enum = cast(Activities.ActivityTimeType as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapAllDay
    ON StringMapAllDay.SourceTable = 'NoYes'
   AND StringMapAllDay.Enum = cast(Activities.AllDay as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapsmmActivityCategory
    ON StringMapsmmActivityCategory.SourceTable = 'smmActivityCategory'
   AND StringMapsmmActivityCategory.Enum = cast(Activities.Category as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapClosed
    ON StringMapClosed.SourceTable = 'NoYes'
   AND StringMapClosed.Enum = cast(Activities.Closed as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapIsMasterAppointment
    ON StringMapIsMasterAppointment.SourceTable = 'NoYes'
   AND StringMapIsMasterAppointment.Enum = cast(Activities.IsMasterAppointment as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapIsTemplate
    ON StringMapIsTemplate.SourceTable = 'NoYes'
   AND StringMapIsTemplate.Enum = cast(Activities.IsTemplate as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapKeepSynchronized
    ON StringMapKeepSynchronized.SourceTable = 'NoYes'
   AND StringMapKeepSynchronized.Enum = cast(Activities.KeepSynchronized as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapModified
    ON StringMapModified.SourceTable = 'NoYes'
   AND StringMapModified.Enum = cast(Activities.Modified as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapRecurrenceState
    ON StringMapRecurrenceState.SourceTable = 'NoYes'
   AND StringMapRecurrenceState.Enum = cast(Activities.RecurrenceState as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapReminderActive
    ON StringMapReminderActive.SourceTable = 'NoYes'
   AND StringMapReminderActive.Enum = cast(Activities.ReminderActive as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapResponseRequested
    ON StringMapResponseRequested.SourceTable = 'NoYes'
   AND StringMapResponseRequested.Enum = cast(Activities.ResponseRequested as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapsmmSensitivity
    ON StringMapsmmSensitivity.SourceTable = 'smmSensitivity'
   AND StringMapsmmSensitivity.Enum = cast(Activities.Sensitivity as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapsmmActivityStatus
    ON StringMapsmmActivityStatus.SourceTable = 'smmActivityStatus'
   AND StringMapsmmActivityStatus.Enum = cast(Activities.Status as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapsmmActivityPriority
    ON StringMapsmmActivityPriority.SourceTable = 'smmActivityPriority'
   AND StringMapsmmActivityPriority.Enum = cast(Activities.TaskPriority as string)

LEFT JOIN {cat}.{sch}.StringMap  AS StringMapsmmTeamTask
    ON StringMapsmmTeamTask.SourceTable = 'NoYes'
   AND StringMapsmmTeamTask.Enum = cast(Activities.TeamTask as string)

WHERE Activities.ActivityNumber <> '';
"""

# COMMAND ----------

# Execute the view creation
spark.sql(view_sql)

# COMMAND ----------

# ------------------------------------------------------------
# 3️⃣  Verify that the view exists
# ------------------------------------------------------------
sample_df = spark.sql(f"SELECT * FROM {cat}.{sch}.V_CaseActivity LIMIT 10")
display(sample_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 6078)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW catalog.schema.V_CaseActivity AS SELECT     -- 1️⃣  Case code (empty → '_N/A')     coalesce(nullif(CaseDetailBase.CaseId, ''), '_N/A')            AS CaseCode,      -- 2️⃣  The activity number (empty → '_N/A')     coalesce(Activities.ActivityNumber, '_N/A')                    AS ActivityNumber,      -- 3️⃣  Dates – force NULL → 1900‑01‑01 and cast to DATE     coalesce(cast(Activities.StartDateTime   AS date), cast('1900-01-01' AS date)) AS StartDateTime,     coalesce(cast(Activities.EndDateTime     AS date), cast('1900-01-01' AS date)) AS EndDateTime,     coalesce(cast(Activities.ActualEndDateTime AS date), cast('1900-01-01' AS date)) AS ActualEndDateTime,      Activities.DataAreaId                                            AS CompanyCode,      -- 4️⃣  Look‑ups from the StringMap tables (empty → '_N/A')     coalesce(nullif(StringMapsmmActivitysmmShowTimeAs.Name, ''), '_N/A') AS ActivityTimeType,     coalesce(nullif(StringMapsmmActivityTaskTimeType.Name, ''), '_N/A')   AS ActivityTaskTimeType,      -- 5️⃣  Numbers – default 0 when NULL     coalesce(Activities.ActualWork, 0)                                 AS ActualWork,     coalesce(nullif(StringMapAllDay.Name, ''), '_N/A')                AS AllDay,     coalesce(nullif(StringMapsmmActivityCategory.Name, ''), '_N/A')   AS Category,     coalesce(nullif(StringMapClosed.Name, ''), '_N/A')                AS Closed,     coalesce(nullif(HCMDoneByWorker.Name, ''), '_N/A')                AS DoneByWorker,     coalesce(Activities.PercentageCompleted, 0)                       AS PercentageCompleted,     coalesce(nullif(Activities.Purpose, ''), '_N/A')                  AS Purpose,     coalesce(nullif(HCMResponsibleWorker.Name, ''), '_N/A')           AS ResponsibleWorker,     coalesce(nullif(StringMapsmmActivityStatus.Name, ''), '_N/A')     AS Status,     coalesce(nullif(Activities.TypeId, ''), '_N/A')                   AS TypeCode,     coalesce(nullif(Activities.UserMemo, ''), '_N/A')                AS UserMemo  FROM catalog.schema.SMRBICaseDetailBaseStaging   AS CaseDetailBase LEFT JOIN catalog.schema.SMRBIsmmActivityParentLinkTableStaging  AS ActivityParentLinkTable     ON CaseDetailBase.CaseDetailBaseRecId = ActivityParentLinkTable.RefRecId    AND CaseDetailBase.DataAreaId          = ActivityParentLinkTable.DataAreaId  LEFT JOIN catalog.schema.SMRBIsmmActivitiesStaging  AS Activities     ON ActivityParentLinkTable.ActivityNumber = Activities.ActivityNumber  LEFT JOIN catalog.schema.SMRBIHcmWorkerStaging      AS HCMDoneByWorker     ON HCMDoneByWorker.PersonnelNumber = Activities.DoneByWorker_PersonnelNumber  LEFT JOIN catalog.schema.SMRBIHcmWorkerStaging      AS HCMResponsibleWorker     ON HCMResponsibleWorker.PersonnelNumber = Activities.DoneByWorker_PersonnelNumber  LEFT JOIN catalog.schema.StringMap  AS StringMapsmmActivityTaskTimeType     ON StringMapsmmActivityTaskTimeType.SourceTable = 'smmActivityTaskTimeType'    AND StringMapsmmActivityTaskTimeType.Enum = cast(Activities.ActivityTimeType as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapsmmActivitysmmShowTimeAs     ON StringMapsmmActivitysmmShowTimeAs.SourceTable = 'StringMapsmmActivitysmmShowTimeAs'    AND StringMapsmmActivitysmmShowTimeAs.Enum = cast(Activities.ActivityTimeType as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapAllDay     ON StringMapAllDay.SourceTable = 'NoYes'    AND StringMapAllDay.Enum = cast(Activities.AllDay as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapsmmActivityCategory     ON StringMapsmmActivityCategory.SourceTable = 'smmActivityCategory'    AND StringMapsmmActivityCategory.Enum = cast(Activities.Category as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapClosed     ON StringMapClosed.SourceTable = 'NoYes'    AND StringMapClosed.Enum = cast(Activities.Closed as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapIsMasterAppointment     ON StringMapIsMasterAppointment.SourceTable = 'NoYes'    AND StringMapIsMasterAppointment.Enum = cast(Activities.IsMasterAppointment as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapIsTemplate     ON StringMapIsTemplate.SourceTable = 'NoYes'    AND StringMapIsTemplate.Enum = cast(Activities.IsTemplate as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapKeepSynchronized     ON StringMapKeepSynchronized.SourceTable = 'NoYes'    AND StringMapKeepSynchronized.Enum = cast(Activities.KeepSynchronized as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapModified     ON StringMapModified.SourceTable = 'NoYes'    AND StringMapModified.Enum = cast(Activities.Modified as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapRecurrenceState     ON StringMapRecurrenceState.SourceTable = 'NoYes'    AND StringMapRecurrenceState.Enum = cast(Activities.RecurrenceState as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapReminderActive     ON StringMapReminderActive.SourceTable = 'NoYes'    AND StringMapReminderActive.Enum = cast(Activities.ReminderActive as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapResponseRequested     ON StringMapResponseRequested.SourceTable = 'NoYes'    AND StringMapResponseRequested.Enum = cast(Activities.ResponseRequested as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapsmmSensitivity     ON StringMapsmmSensitivity.SourceTable = 'smmSensitivity'    AND StringMapsmmSensitivity.Enum = cast(Activities.Sensitivity as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapsmmActivityStatus     ON StringMapsmmActivityStatus.SourceTable = 'smmActivityStatus'    AND StringMapsmmActivityStatus.Enum = cast(Activities.Status as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapsmmActivityPriority     ON StringMapsmmActivityPriority.SourceTable = 'smmActivityPriority'    AND StringMapsmmActivityPriority.Enum = cast(Activities.TaskPriority as string)  LEFT JOIN catalog.schema.StringMap  AS StringMapsmmTeamTask     ON StringMapsmmTeamTask.SourceTable = 'NoYes'    AND StringMapsmmTeamTask.Enum = cast(Activities.TeamTask as string)  WHERE Activities.ActivityNumber <> '';
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
