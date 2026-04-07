# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ProductionCapacity.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ProductionCapacity.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------------------------
# Databricks notebook to create the `V_ProductionCapacity` view in Unity Catalog.
# ------------------------------------------------------------------------------------
# All object names are fully qualified with the placeholders `dbe_dbx_internships` and `datastore`.
# Replace these placeholders with the actual catalog and schema names when deploying.
#
# NOTE: 
#   • T‑SQL constructs that have no direct equivalent in Spark SQL are converted
#     to their Spark dialect (e.g. ISNULL → coalesce, GETDATE() → current_timestamp()).
#   • COLLATE clauses and other SQL Server‑only syntax are omitted because they
#     are not supported in Spark. If strict data‑type or collation requirements
#     exist, consider handling them in downstream transformations.
#   • Numeric(38,17) is mapped to DECIMAL(38,17) in Spark.
#   • String literals prefixed with N’’ in T‑SQL are written in plain quotes;
#     Spark treats strings as UTF‑8 by default.
#   • The query is written as a single‑string literal; no need for f‑string
#     interpolation because the catalog/schema placeholders are literal.
# ------------------------------------------------------------------------------------

spark.sql("""

CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_ProductionCapacity` AS

/* ----------------------------------------------
   1. Build a mapping between work calendars and
      resources (resource type = Single / Group)
   ---------------------------------------------- */
WITH ResourceCalendarMapping AS (
    SELECT
        DataAreaId,
        OperationsResourceId,
        'Single' AS ResourceType,
        ValidFrom,
        ValidTo,
        WorkCalendarId
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIOpResOperationsResourceWorkCalendarAssignmentStaging`

    UNION ALL

    SELECT
        DataAreaId,
        OperationsResourceId AS OperationsResourceGroupId,
        'Group' AS ResourceType,
        ValidFrom,
        ValidTo,
        WorkCalendarId
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIOpResOperationsResourceGroupWorkCalendarAssignmentStaging`
),

/* ----------------------------------------------
   2. Build the work‑calendar‑interval set (WCTIS)
      that contains the maximum capacity per day.
   ---------------------------------------------- */
WCTI AS (
    SELECT DISTINCT
        WCD.DataAreaId,
        WCD.WorkCalendarDate,
        WCD.WorkCalendarId,
        COALESCE(WCTIS.MaximumCapacity, WCTIS2.MaximumCapacity, 0) AS MaximumCapacity
    FROM (
        /* Work‑calendar days */
        SELECT DISTINCT
            WorkCalendarId,
            CalendarDate    AS WorkCalendarDate,
            DataAreaId
        FROM `dbe_dbx_internships`.`datastore`.`SMRBIWorkCalendarDayStaging`
    ) WCD

    LEFT JOIN (
        /* Work‑calendar time intervals – base table */
        SELECT
            DataAreaId,
            WorkCalendarDate,
            WorkCalendarId,
            SUM(((EndTime - StartTime) / 3600.0) * EfficiencyPercentage / 100) AS MaximumCapacity
        FROM `dbe_dbx_internships`.`datastore`.`SMRBIWorkCalendarTimeIntervalStaging`
        GROUP BY DataAreaId, WorkCalendarDate, WorkCalendarId
    ) WCTIS
        ON WCD.DataAreaId     = WCTIS.DataAreaId
       AND WCD.WorkCalendarId = WCTIS.WorkCalendarId
       AND WCD.WorkCalendarDate = WCTIS.WorkCalendarDate

    LEFT JOIN (
        /* Optional base‑calendar lookup */
        SELECT
            DataAreaId,
            WorkCalendarDate,
            WorkCalendarId,
            SUM(((EndTime - StartTime) / 3600.0) * EfficiencyPercentage / 100) AS MaximumCapacity
        FROM `dbe_dbx_internships`.`datastore`.`SMRBIWorkCalendarTimeIntervalStaging`
        GROUP BY DataAreaId, WorkCalendarDate, WorkCalendarId
    ) WCTIS2
        ON WCD.DataAreaId     = WCTIS2.DataAreaId
       AND WCD.WorkCalendarDate = WCTIS2.WorkCalendarDate
       AND WCD.WorkCalendarId   = WCTIS2.WorkCalendarId
)

/* ----------------------------------------------
   3. First part of the final result – resources
   ---------------------------------------------- */
, FirstPart AS (
    SELECT
        CONCAT(
            RCM.DataAreaId,
            WCTI.WorkCalendarDate,
            RCM.WorkCalendarId,
            RCM.OperationsResourceId
        )                                   AS ProductionCapacityIdScreening,
        RCM.DataAreaId                        AS CompanyCode,
        COALESCE(RPVS.ReqPlanId, '_N/A')       AS PlanVersion,
        COALESCE(WCTI.WorkCalendarDate, DATE '1900-01-01') AS CapacityDate,
        COALESCE(RCM.WorkCalendarId, '_N/A')  AS CalendarCode,
        COALESCE(RCM.OperationsResourceId, '_N/A') AS ResourceCode,
        'Max. Capacity'                       AS RefType,
        '_N/A'                                AS RefCode,
        CASE WHEN RCM.ResourceType = 'Group' THEN 0
             ELSE COALESCE(ROUND(WCTI.MaximumCapacity, 3), 0)
        END                                      AS MaximumCapacity,
        CAST(0 AS DECIMAL(38,17))               AS ReservedCapacity,
        CAST(0 AS DECIMAL(38,17))               AS AvailableCapacity
    FROM ResourceCalendarMapping RCM
    INNER JOIN `dbe_dbx_internships`.`datastore`.`SMRBIReqPlanVersionStaging` RPVS
        ON RCM.DataAreaId = RPVS.ReqPlanDataAreaId
    LEFT JOIN WCTI
        ON RCM.DataAreaId    = WCTI.DataAreaId
       AND RCM.WorkCalendarId = WCTI.WorkCalendarId
       AND WCTI.WorkCalendarDate BETWEEN RCM.ValidFrom AND RCM.ValidTo
),

/* ----------------------------------------------
   4. Second part of the final result – work‑center resource
   ---------------------------------------------- */
SecondPart AS (
    SELECT
        CONCAT(
            WCCRS.DataAreaId,
            WCCRS.TransDate,
            WCCRS.WorkCalendarId,
            WCCRS.WrkCtrId
        )                                      AS ProductionCapacityIdScreening,
        WCCRS.DataAreaId                        AS CompanyId,
        COALESCE(WCCRS.ReqPlanId, '_N/A')       AS PlanVersion,
        COALESCE(WCCRS.TransDate, DATE '1900-01-01') AS CapacityDate,
        COALESCE(WCCRS.WorkCalendarId, '_N/A')  AS CalendarId,
        COALESCE(WCCRS.WrkCtrId, '_N/A')        AS ResourceId,
        COALESCE(WCCRS.RefType, '_N/A')          AS RefType,
        COALESCE(WCCRS.RefId, '_N/A')           AS RefId,
        CAST(0 AS DECIMAL(38,17))               AS MaximumCapacity,
        COALESCE(WCCRS.ReservedCapacity, 0)    AS ReservedCapacity,
        CAST(0 AS DECIMAL(38,17))               AS AvailableCapacity
    FROM (
        /* Intermediate aggregation */
        SELECT
            WCCRS.DataAreaId,
            RPVS.ReqPlanId,
            SM.Name                      AS RefType,
            WCCRS.RefId,
            WCCRS.TransDate,
            WCCRS.WrkCtrId,
            WCCRS.WrkCtrLoadPct,
            COALESCE(SUM(ROUND(WCCRS.WrkCtrSec / 3600.0, 3)), 0) AS ReservedCapacity,
            COALESCE(ORORWCAS.WorkCalendarId, ORORGWCAS.WorkCalendarId) AS WorkCalendarId,
            COALESCE(WCCRS.ReqPlanId, '_N/A') AS ReqPlanId
        FROM `dbe_dbx_internships`.`datastore`.`SMRBIWrkCtrCapResStaging` WCCRS
        LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIReqPlanVersionStaging` RPVS
            ON WCCRS.PlanVersion = RPVS.ReqPlanVersionRecId
           AND WCCRS.DataAreaId  = RPVS.ReqPlanDataAreaId
        LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` SM
            ON SM.SourceTable   = 'SMRBIWrkCtrCapResStaging'
           AND SM.SourceColumn  = 'RefType'
           AND SM.Enum          = WCCRS.RefType
        LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIOpResOperationsResourceWorkCalendarAssignmentStaging` ORORWCAS
            ON WCCRS.DataAreaId      = ORORWCAS.DataAreaId
           AND WCCRS.WrkCtrId        = ORORWCAS.OperationsResourceId
           AND current_timestamp()   BETWEEN ORORWCAS.ValidFrom AND ORORWCAS.ValidTo
        LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIOpResOperationsResourceGroupWorkCalendarAssignmentStaging` ORORGWCAS
            ON WCCRS.DataAreaId      = ORORGWCAS.DataAreaId
           AND WCCRS.WrkCtrId        = ORORGWCAS.OperationsResourceGroupId
           AND current_timestamp()   BETWEEN ORORGWCAS.ValidFrom AND ORORGWCAS.ValidTo
        WHERE 1 = 1
        GROUP BY
            WCCRS.DataAreaId,
            RPVS.ReqPlanId,
            WCCRS.RefId,
            SM.Name,
            WCCRS.TransDate,
            WCCRS.WrkCtrId,
            WCCRS.WrkCtrLoadPct,
            COALESCE(ORORWCAS.WorkCalendarId, ORORGWCAS.WorkCalendarId)
    ) WCCRS
)

SELECT * FROM FirstPart
UNION ALL
SELECT * FROM SecondPart;

""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
