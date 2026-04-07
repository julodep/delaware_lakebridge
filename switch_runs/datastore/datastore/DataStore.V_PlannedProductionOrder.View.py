# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_PlannedProductionOrder.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_PlannedProductionOrder.View.sql`

# COMMAND ----------

# ---------------------------------------------------------------
# Spark view creation – conversion of a T‑SQL view into a Unity
# Catalog view.  The view name is *V_PlannedProductionOrder*.
#
# All object references are fully–qualified:
#   `dbe_dbx_internships`.`datastore`.<object>
#
# The T‑SQL functions and syntax that have direct Spark equivalents
# are mapped as follows:
#    ISNULL(expr, rep)   -> NVL(expr, rep)
#    NULLIF(a,b)         -> NULLIF(a,b)
#    DATEADD(unit, n, d)-> DATE_ADD(d, n)   (unit = DD)
#    UPPER(x)            -> UPPER(x)
#    CAST(v AS NVARCHAR) -> CAST(v AS STRING)
#  Other constructs (e.g., `FROM <table> AS alias` or joins) are kept
#  unchanged but use backticks and no square brackets.
# ---------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_PlannedProductionOrder` AS
SELECT
    /* 1. Primary keys and identifiers ---------------------------------------------------*/
    ReqPO.RefId                                   AS PlannedProductionOrderCode,

    /* 2. Product identification with default fallback -----------------------------------*/
    NVL(NULLIF(ReqPO.ItemId, ''), '_N/A')         AS ProductCode,
    NVL(NULLIF(UPPER(ReqPO.DataAreaId), ''), '_N/A') AS CompanyCode,
    NVL(NULLIF(ReqPO.CovInventDimId, ''), '_N/A') AS ProductConfigurationCode,

    /* 3. Production order reference -----------------------------------------------------*/
    NVL(NULLIF(ProdTable.ProdId, ''), '_N/A')      AS ProductionOrderCode,

    /* 4. Requirement / order dates -------------------------------------------------------*/
    ReqPO.ReqDate                                  AS RequirementDate,
    ReqPO.ReqDateDLV                               AS RequestedDate,
    ReqPO.ReqDateOrder                             AS OrderDate,

    /* 5. Delivery date – T‑SQL DATEADD(DD, ...) -> Spark DATE_ADD(d, n) */
    DATE_ADD(
        CAST(ReqPO.ReqDateOrder AS DATE),          -- start date
        IFNULL(ReqPO.LeadTime, 0)                  -- days to add
    )                                               AS DeliveryDate,

    /* 6. Status handling – cast to string and provide default --------------------------------*/
    CAST(NVL(NULLIF(StringMapReqPOStatus.Name, ''), '_N/A') AS STRING) AS Status,

    /* 7. Lead time and inventory unit ----------------------------------------------*/
    IFNULL(ReqPO.LeadTime, 0)                      AS LeadTime,
    NVL(NULLIF(InventTableModule.UnitId, ''), '_N/A') AS InventoryUnit,

    /* 8. Quantities ---------------------------------------------------------------------*/
    ReqPO.QTY                                     AS RequirementQuantity,
    NVL(NULLIF(ReqPO.PurchUNIT, ''), '_N/A')      AS PurchaseUnit,
    ReqPO.PurchQTY                                 AS PurchaseQuantity

FROM    `dbe_dbx_internships`.`datastore`.`SMRBIReqPOStaging`       AS ReqPO
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIInventTableModuleStaging` AS InventTableModule
          ON InventTableModule.ItemId   = ReqPO.ItemId
         AND InventTableModule.DataAreaId = ReqPO.DataAreaId
         AND InventTableModule.ModuleType = '0'          -- `ModuleType` is a string literal

LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIProdTableStaging` AS ProdTable
          ON ProdTable.COMPANY   = ReqPO.COMPANY
         AND ProdTable.ReqPOId   = ReqPO.RefId

LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` AS StringMapReqPOStatus
          ON StringMapReqPOStatus.SourceTable = 'ReqPOStatus'
         AND StringMapReqPOStatus.Enum       = CAST(ReqPO.ReqPOStatus AS STRING)

WHERE 1=1
  AND ReqPO.RefType = '31'   -- filter to the desired record type
;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2910)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_PlannedProductionOrder` AS SELECT     /* 1. Primary keys and identifiers ---------------------------------------------------*/     ReqPO.RefId                                   AS PlannedProductionOrderCode,      /* 2. Product identification with default fallback -----------------------------------*/     NVL(NULLIF(ReqPO.ItemId, ''), '_N/A')         AS ProductCode,     NVL(NULLIF(UPPER(ReqPO.DataAreaId), ''), '_N/A') AS CompanyCode,     NVL(NULLIF(ReqPO.CovInventDimId, ''), '_N/A') AS ProductConfigurationCode,      /* 3. Production order reference -----------------------------------------------------*/     NVL(NULLIF(ProdTable.ProdId, ''), '_N/A')      AS ProductionOrderCode,      /* 4. Requirement / order dates -------------------------------------------------------*/     ReqPO.ReqDate                                  AS RequirementDate,     ReqPO.ReqDateDLV                               AS RequestedDate,     ReqPO.ReqDateOrder                             AS OrderDate,      /* 5. Delivery date – T‑SQL DATEADD(DD, ...) -> Spark DATE_ADD(d, n) */     DATE_ADD(         CAST(ReqPO.ReqDateOrder AS DATE),          -- start date         IFNULL(ReqPO.LeadTime, 0)                  -- days to add     )                                               AS DeliveryDate,      /* 6. Status handling – cast to string and provide default --------------------------------*/     CAST(NVL(NULLIF(StringMapReqPOStatus.Name, ''), '_N/A') AS STRING) AS Status,      /* 7. Lead time and inventory unit ----------------------------------------------*/     IFNULL(ReqPO.LeadTime, 0)                      AS LeadTime,     NVL(NULLIF(InventTableModule.UnitId, ''), '_N/A') AS InventoryUnit,      /* 8. Quantities ---------------------------------------------------------------------*/     ReqPO.QTY                                     AS RequirementQuantity,     NVL(NULLIF(ReqPO.PurchUNIT, ''), '_N/A')      AS PurchaseUnit,     ReqPO.PurchQTY                                 AS PurchaseQuantity  FROM    `_placeholder_`.`_placeholder_`.`SMRBIReqPOStaging`       AS ReqPO LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIInventTableModuleStaging` AS InventTableModule           ON InventTableModule.ItemId   = ReqPO.ItemId          AND InventTableModule.DataAreaId = ReqPO.DataAreaId          AND InventTableModule.ModuleType = '0'          -- `ModuleType` is a string literal  LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIProdTableStaging` AS ProdTable           ON ProdTable.COMPANY   = ReqPO.COMPANY          AND ProdTable.ReqPOId   = ReqPO.RefId  LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` AS StringMapReqPOStatus           ON StringMapReqPOStatus.SourceTable = 'ReqPOStatus'          AND StringMapReqPOStatus.Enum       = CAST(ReqPO.ReqPOStatus AS STRING)  WHERE 1=1   AND ReqPO.RefType = '31'   -- filter to the desired record type ;
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
