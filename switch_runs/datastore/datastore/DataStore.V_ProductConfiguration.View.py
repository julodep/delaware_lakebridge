# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ProductConfiguration.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ProductConfiguration.View.sql`

# COMMAND ----------

# ---------------------------------------------
# Create (or replace) the view V_ProductConfiguration
# All references are fully‑qualified:
#   dbe_dbx_internships.datastore.TableName
# ---------------------------------------------

# Use an f‑string so that `dbe_dbx_internships` and `datastore` are replaced by
# the corresponding Python variables before Spark parses the SQL.
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_ProductConfiguration` AS

SELECT
    -- 1. Company code – convert to upper‑case
    upper(id.DataAreaId)                                      AS CompanyCode,

    -- 2. InventDimCode
    id.InventDimId                                            AS InventDimCode,

    -- 3. Product configuration code – fall back to '_N/A' if NULL or empty
    coalesce(nullif(id.ConfigId, ''), '_N/A')                 AS ProductConfigurationCode,

    -- 4. Invent batch code – upper‑case, fall back to '_N/A'
    coalesce(nullif(upper(id.InventBatchId), ''), '_N/A')     AS InventBatchCode,

    -- 5. Invent color code – fall back to '_N/A'
    coalesce(nullif(id.InventColorId, ''), '_N/A')            AS InventColorCode,

    -- 6. Invent size code – fall back to '_N/A'
    coalesce(nullif(id.InventSizeId, ''), '_N/A')             AS InventSizeCode,

    -- 7. Invent style code – fall back to '_N/A'
    coalesce(nullif(id.InventStyleId, ''), '_N/A')            AS InventStyleCode,

    -- 8. Invent status code – fall back to '_N/A'
    coalesce(nullif(id.InventStatusId, ''), '_N/A')           AS InventStatusCode,

    -- 9. Site code – fall back to '_N/A'
    coalesce(nullif(id.InventSiteId, ''), '_N/A')             AS SiteCode,

    -- 10. Site name – upper‑case, fall back to '_N/A'
    coalesce(upper(iss.Name), '_N/A')                         AS SiteName,

    -- 11. Warehouse code – fall back to '_N/A'
    coalesce(nullif(id.InventLocationId, ''), '_N/A')         AS WarehouseCode,

    -- 12. Warehouse name – upper‑case, fall back to '_N/A'
    coalesce(upper(iws.WarehouseName), '_N/A')                AS WarehouseName,

    -- 13. Warehouse location code – fall back to '_N/A'
    coalesce(nullif(id.WMSLocationId, ''), '_N/A')            AS WarehouseLocationCode

FROM `dbe_dbx_internships`.`datastore`.`SMRBIInventDimStaging` id

-- Join to the warehouse staging table
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIInventWarehouseStaging` iws
    ON iws.WarehouseId      = id.InventLocationId
   AND iws.DataAreaId       = id.DataAreaId

-- Join to the site staging table
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIInventSiteStaging` iss
    ON iss.SiteId          = id.InventSiteId
   AND iss.DataAreaId      = id.DataAreaId;
""" )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2307)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_ProductConfiguration` AS  SELECT     -- 1. Company code – convert to upper‑case     upper(id.DataAreaId)                                      AS CompanyCode,      -- 2. InventDimCode     id.InventDimId                                            AS InventDimCode,      -- 3. Product configuration code – fall back to '_N/A' if NULL or empty     coalesce(nullif(id.ConfigId, ''), '_N/A')                 AS ProductConfigurationCode,      -- 4. Invent batch code – upper‑case, fall back to '_N/A'     coalesce(nullif(upper(id.InventBatchId), ''), '_N/A')     AS InventBatchCode,      -- 5. Invent color code – fall back to '_N/A'     coalesce(nullif(id.InventColorId, ''), '_N/A')            AS InventColorCode,      -- 6. Invent size code – fall back to '_N/A'     coalesce(nullif(id.InventSizeId, ''), '_N/A')             AS InventSizeCode,      -- 7. Invent style code – fall back to '_N/A'     coalesce(nullif(id.InventStyleId, ''), '_N/A')            AS InventStyleCode,      -- 8. Invent status code – fall back to '_N/A'     coalesce(nullif(id.InventStatusId, ''), '_N/A')           AS InventStatusCode,      -- 9. Site code – fall back to '_N/A'     coalesce(nullif(id.InventSiteId, ''), '_N/A')             AS SiteCode,      -- 10. Site name – upper‑case, fall back to '_N/A'     coalesce(upper(iss.Name), '_N/A')                         AS SiteName,      -- 11. Warehouse code – fall back to '_N/A'     coalesce(nullif(id.InventLocationId, ''), '_N/A')         AS WarehouseCode,      -- 12. Warehouse name – upper‑case, fall back to '_N/A'     coalesce(upper(iws.WarehouseName), '_N/A')                AS WarehouseName,      -- 13. Warehouse location code – fall back to '_N/A'     coalesce(nullif(id.WMSLocationId, ''), '_N/A')            AS WarehouseLocationCode  FROM `_placeholder_`.`_placeholder_`.`SMRBIInventDimStaging` id  -- Join to the warehouse staging table LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIInventWarehouseStaging` iws     ON iws.WarehouseId      = id.InventLocationId    AND iws.DataAreaId       = id.DataAreaId  -- Join to the site staging table LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIInventSiteStaging` iss     ON iss.SiteId          = id.InventSiteId    AND iss.DataAreaId      = id.DataAreaId;
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
