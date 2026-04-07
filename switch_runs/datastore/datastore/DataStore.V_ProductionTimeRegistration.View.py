# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ProductionTimeRegistration.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ProductionTimeRegistration.View.sql`

# COMMAND ----------

# 👉  Create a persistent view that mirrors the T‑SQL `CREATE VIEW` statement.  
#      All object names are fully‑qualified with `dbe_dbx_internships` and `datastore`  
#      (replace these placeholders with your actual catalog / schema names).  
# 
#      The view expression was translated from T‑SQL functions to Spark‑SQL
#      equivalents:
#        • `ISNULL` → `nvl`
#        • `NULLIF` → `nullif`
#        • `UPPER`  → `upper`
#        • `CAST(... AS DATE)` → `coalesce(..., ...)::date`
#        • Default constants (`'_N/A'`, `'N_A'`, `-1`) are kept as literals.
#      All joins and column aliases are preserved.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------
# 1. View definition – replace `dbe_dbx_internships` & `datastore`
# ---------------------------------------------------------
view_sql = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_ProductionTimeRegistration` AS
SELECT
    /* Production order key – fallback to '_N/A' if NULL or empty */
    nvl(nullif(PRT.TransRefId, ''), '_N/A') AS ProductionOrderCode,

    /* Company code – upper‑cased, fallback '_N/A' */
    nvl(nullif(upper(PRT.Company), ''), '_N/A') AS CompanyCode,

    /* Product configuration – upper‑cased, fallback '_N/A' */
    nvl(nullif(upper(PRT.InventDimId), ''), '_N/A') AS ProductConfigurationCode,

    /* Route code – fallback '_N/A' */
    nvl(nullif(PRT.RouteId, ''), '_N/A') AS RouteCode,

    /* Routing name – fallback '_N/A' */
    nvl(nullif(RHS.RouteName, ''), '_N/A') AS RoutingName,

    /* Resource / work‑centre code – fallback '_N/A' */
    nvl(nullif(PRT.WrkCtrId, ''), '_N/A') AS ResourceCode,

    /* Operation code – fallback '_N/A' */
    nvl(nullif(PRT.OPRId, ''), '_N/A') AS OperationCode,

    /* Operation number – coalesce to -1 if NULL */
    coalesce(PRT.OPRNum, -1) AS OperationNumber,

    /* Constant shift value – matches original 'N_A' literal */
    'N_A' AS Shift,

    /* Operator type – fallback '_N/A' */
    nvl(nullif(PRT.CategoryId, ''), '_N/A') AS OperatorType,

    /* Operator name – coalesce to -1 if NULL */
    coalesce(PRT.Worker, -1) AS OperatorName,

    /* Primary key / identifier */
    PRT.ProdRouteTransRecId AS RecId,

    /* Posted journal date – use '1900‑01‑01' when NULL */
    coalesce(PRT.DateWip, to_date('1900-01-01', 'yyyy-MM-dd')) AS PostedJournalDate,

    /* Hours and hourly price – cast to original types (kept as-is) */
    PRT.Hours,
    PRT.HourPrice
FROM `dbe_dbx_internships`.`datastore`.`SMRBIProdRouteTransStaging` PRT
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIProdTableStaging` PT
       ON PT.Company   = PRT.Company
      AND PT.ProdId    = PRT.TransRefId
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIRouteHeaderStaging` RHS
       ON RHS.Company = PRT.Company
      AND RHS.RouteId = PT.RouteId
"""

# COMMAND ----------

# Execute the view creation
spark.sql(view_sql)

# COMMAND ----------

# ---------------------------------------------------------------------------
# 2. Validate the view by showing a sample of rows (optional)
# ---------------------------------------------------------------------------

display(spark.sql(f"SELECT * FROM `dbe_dbx_internships`.`datastore`.`V_ProductionTimeRegistration` LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
