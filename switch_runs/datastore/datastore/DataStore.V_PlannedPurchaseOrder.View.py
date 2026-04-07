# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_PlannedPurchaseOrder.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_PlannedPurchaseOrder.View.sql`

# COMMAND ----------

# ----------------------------------------------
# Databricks Notebook – Convert T‑SQL VIEW to a
# fully‑qualified Unity Catalog view.
#
# Replace the placeholder values of *catalog* and *schema*
# with the actual catalog and schema names used in your workspace.
# ----------------------------------------------

# Define the catalog and schema (replace with real names)
catalog = "your_catalog_name"   # e.g., "spark_catalog"
schema  = "your_schema_name"    # e.g., "default"

# COMMAND ----------

# ------------------------------------------------------------------
# Create (or replace) the view V_PlannedPurchaseOrder with the same
# semantics as the original T‑SQL statement.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.V_PlannedPurchaseOrder AS
SELECT
    -- Original column name: ReqPO.RefId
    ReqPO.RefId                                            AS PlannedPurchaseOrderCode,

    -- ISNULL(NULLIF(ReqPO.ItemId,'') ,'_N/A')
    COALESCE(NULLIF(ReqPO.ItemId, ''), '_N/A')              AS ProductCode,

    -- ISNULL(NULLIF(ReqPO.VendId,'') ,'_N/A')
    COALESCE(NULLIF(ReqPO.VendId, ''), '_N/A')              AS SupplierCode,

    -- ISNULL(NULLIF(UPPER(ReqPO.DataAreaId),''), '_N/A')
    COALESCE(NULLIF(UPPER(ReqPO.DataAreaId), ''), '_N/A')    AS CompanyCode,

    -- ISNULL(NULLIF(ReqPO.CovInventDimId,''), '_N/A')
    COALESCE(NULLIF(ReqPO.CovInventDimId, ''), '_N/A')       AS ProductConfigurationCode,

    -- ISNULL(NULLIF(ReqPO.PurchId,''), '_N/A')
    COALESCE(NULLIF(ReqPO.PurchId, ''), '_N/A')             AS PurchaseOrderCode,

    -- Direct mappings
    ReqPO.ReqDate                                          AS RequirementDate,
    ReqPO.ReqDateDLV                                       AS RequestedDate,
    ReqPO.ReqDateOrder                                     AS OrderDate,

    -- DATEADD(DD, ISNULL(ReqPO.LeadTime,0), ReqPO.ReqDateOrder)
    DATE_ADD(ReqPO.ReqDateOrder, COALESCE(ReqPO.LeadTime, 0)) AS DeliveryDate,

    -- CAST(ISNULL(NULLIF(StringMapReqPOStatus.Name,''),'_N/A') AS NVARCHAR(50))
    CAST(COALESCE(NULLIF(StringMapReqPOStatus.Name, ''), '_N/A') AS STRING) AS Status,

    -- ISNULL(ReqPO.LeadTime,0)
    COALESCE(ReqPO.LeadTime, 0)                            AS LeadTime,

    -- ISNULL(NULLIF(InventTableModule.UnitId, ''), '_N/A')
    COALESCE(NULLIF(InventTableModule.UnitId, ''), '_N/A') AS InventoryUnit,

    -- Direct mappings
    ReqPO.QTY                                              AS RequirementQuantity,
    COALESCE(NULLIF(ReqPO.PurchUNIT, ''), '_N/A')           AS PurchaseUnit,
    ReqPO.PurchQTY                                         AS PurchaseQuantity

FROM `dbe_dbx_internships`.`dbo`.SMRBIReqPOStaging AS ReqPO

-- Left join to the InventoryModule table
LEFT JOIN `dbe_dbx_internships`.`dbo`.SMRBIInventTableModuleStaging AS InventTableModule
  ON InventTableModule.ItemId   = ReqPO.ItemId
 AND InventTableModule.DataAreaId = ReqPO.DataAreaId
 AND InventTableModule.ModuleType = '0'

-- Left join to the StringMap lookup table
LEFT JOIN `dbe_dbx_internships`.`ETL`.StringMap AS StringMapReqPOStatus
  ON StringMapReqPOStatus.SourceTable = 'ReqPOStatus'
  AND StringMapReqPOStatus.Enum  = CAST(ReqPO.ReqPOStatus AS STRING)

WHERE ReqPO.RefType = '33';
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Preview the first few rows to confirm the view was created
# correctly.  Remove or comment out in production.
# ------------------------------------------------------------------
df = spark.table(f"dbe_dbx_internships.datastore.V_PlannedPurchaseOrder")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2561)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `your_catalog_name`.`your_schema_name`.V_PlannedPurchaseOrder AS SELECT     -- Original column name: ReqPO.RefId     ReqPO.RefId                                            AS PlannedPurchaseOrderCode,      -- ISNULL(NULLIF(ReqPO.ItemId,'') ,'_N/A')     COALESCE(NULLIF(ReqPO.ItemId, ''), '_N/A')              AS ProductCode,      -- ISNULL(NULLIF(ReqPO.VendId,'') ,'_N/A')     COALESCE(NULLIF(ReqPO.VendId, ''), '_N/A')              AS SupplierCode,      -- ISNULL(NULLIF(UPPER(ReqPO.DataAreaId),''), '_N/A')     COALESCE(NULLIF(UPPER(ReqPO.DataAreaId), ''), '_N/A')    AS CompanyCode,      -- ISNULL(NULLIF(ReqPO.CovInventDimId,''), '_N/A')     COALESCE(NULLIF(ReqPO.CovInventDimId, ''), '_N/A')       AS ProductConfigurationCode,      -- ISNULL(NULLIF(ReqPO.PurchId,''), '_N/A')     COALESCE(NULLIF(ReqPO.PurchId, ''), '_N/A')             AS PurchaseOrderCode,      -- Direct mappings     ReqPO.ReqDate                                          AS RequirementDate,     ReqPO.ReqDateDLV                                       AS RequestedDate,     ReqPO.ReqDateOrder                                     AS OrderDate,      -- DATEADD(DD, ISNULL(ReqPO.LeadTime,0), ReqPO.ReqDateOrder)     DATE_ADD(ReqPO.ReqDateOrder, COALESCE(ReqPO.LeadTime, 0)) AS DeliveryDate,      -- CAST(ISNULL(NULLIF(StringMapReqPOStatus.Name,''),'_N/A') AS NVARCHAR(50))     CAST(COALESCE(NULLIF(StringMapReqPOStatus.Name, ''), '_N/A') AS STRING) AS Status,      -- ISNULL(ReqPO.LeadTime,0)     COALESCE(ReqPO.LeadTime, 0)                            AS LeadTime,      -- ISNULL(NULLIF(InventTableModule.UnitId, ''), '_N/A')     COALESCE(NULLIF(InventTableModule.UnitId, ''), '_N/A') AS InventoryUnit,      -- Direct mappings     ReqPO.QTY                                              AS RequirementQuantity,     COALESCE(NULLIF(ReqPO.PurchUNIT, ''), '_N/A')           AS PurchaseUnit,     ReqPO.PurchQTY                                         AS PurchaseQuantity  FROM `your_catalog_name`.`dbo`.SMRBIReqPOStaging AS ReqPO  -- Left join to the InventoryModule table LEFT JOIN `your_catalog_name`.`dbo`.SMRBIInventTableModuleStaging AS InventTableModule   ON InventTableModule.ItemId   = ReqPO.ItemId  AND InventTableModule.DataAreaId = ReqPO.DataAreaId  AND InventTableModule.ModuleType = '0'  -- Left join to the StringMap lookup table LEFT JOIN `your_catalog_name`.`ETL`.StringMap AS StringMapReqPOStatus   ON StringMapReqPOStatus.SourceTable = 'ReqPOStatus'   AND StringMapReqPOStatus.Enum  = CAST(ReqPO.ReqPOStatus AS STRING)  WHERE ReqPO.RefType = '33';
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
