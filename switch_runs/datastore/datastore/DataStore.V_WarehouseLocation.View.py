# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_WarehouseLocation.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_WarehouseLocation.View.sql`

# COMMAND ----------

# Define real catalog and schema names (replace with your actual names)
catalog = "my_catalog"
schema = "my_schema"

# COMMAND ----------

# Create or replace the view in Databricks
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_WarehouseLocation` AS
SELECT
    -- 1️⃣  RecId – direct copy of the underlying column
    WWLS.WarehouseLocationRecId AS RecId,

    -- 2️⃣  CompanyCode – upper‑casing the data area id
    upper(WWLS.DataAreaId) AS CompanyCode,

    -- 3️⃣  WarehouseLocationCode – direct copy
    WWLS.WarehouseLocationId AS WarehouseLocationCode,

    -- 4️⃣  WarehouseCode – direct copy
    IWS.WarehouseId AS WarehouseCode,

    -- 5️⃣  WarehouseName – replace NULL with the literal “_N/A”
    nvl(IWS.WarehouseName, '_N/A') AS WarehouseName,

    -- 6️⃣  WarehouseCodeName – concatenate id + “ - ” + name
    concat_ws(
        ' - ',
        IWS.WarehouseId,
        nvl(IWS.WarehouseName, '_N/A')
    ) AS WarehouseCodeName,

    -- 7️⃣  WareHouseLocationType – trimmed NULL/empty becomes “_N/A”
    nvl(
        nullif(WWLS.WareHouseLocationProfileId, ''),
        '_N/A'
    ) AS WareHouseLocationType
FROM `dbe_dbx_internships`.`datastore`.SMRBIWMSWarehouseLocationStaging WWLS
LEFT JOIN `dbe_dbx_internships`.`datastore`.SMRBIInventWarehouseStaging IWS
       ON  WWLS.WarehouseId   = IWS.WarehouseId
       AND WWLS.DataAreaId   = IWS.DataAreaId
""")

# COMMAND ----------

# Optional: verify the view was created
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`datastore`.`V_WarehouseLocation`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1181)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `my_catalog`.`my_schema`.`V_WarehouseLocation` AS SELECT     -- 1️⃣  RecId – direct copy of the underlying column     WWLS.WarehouseLocationRecId AS RecId,      -- 2️⃣  CompanyCode – upper‑casing the data area id     upper(WWLS.DataAreaId) AS CompanyCode,      -- 3️⃣  WarehouseLocationCode – direct copy     WWLS.WarehouseLocationId AS WarehouseLocationCode,      -- 4️⃣  WarehouseCode – direct copy     IWS.WarehouseId AS WarehouseCode,      -- 5️⃣  WarehouseName – replace NULL with the literal “_N/A”     nvl(IWS.WarehouseName, '_N/A') AS WarehouseName,      -- 6️⃣  WarehouseCodeName – concatenate id + “ - ” + name     concat_ws(         ' - ',         IWS.WarehouseId,         nvl(IWS.WarehouseName, '_N/A')     ) AS WarehouseCodeName,      -- 7️⃣  WareHouseLocationType – trimmed NULL/empty becomes “_N/A”     nvl(         nullif(WWLS.WareHouseLocationProfileId, ''),         '_N/A'     ) AS WareHouseLocationType FROM `my_catalog`.`my_schema`.SMRBIWMSWarehouseLocationStaging WWLS LEFT JOIN `my_catalog`.`my_schema`.SMRBIInventWarehouseStaging IWS        ON  WWLS.WarehouseId   = IWS.WarehouseId        AND WWLS.DataAreaId   = IWS.DataAreaId
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
