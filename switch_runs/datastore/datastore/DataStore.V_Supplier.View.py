# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Supplier.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Supplier.View.sql`

# COMMAND ----------

# Create the V_Supplier view in the target catalog & schema
# ----------------------------------------------------------------------
sql = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_Supplier` AS
SELECT
    -- 1. Vendor table columns
    VVS.VendTableRecId        AS SupplierId,
    upper(VVS.DataAreaId)     AS CompanyCode,
    upper(VVS.VendorAccountNumber) AS SupplierCode,

    -- 2. Supplier name, fallback to '_N/A' if NULL or empty
    coalesce(VVS.Name, '_N/A') AS SupplierName,

    -- 3. Supplier code name (account number + ' - ' + name)
    concat_ws(' - ',
              upper(VVS.VendorAccountNumber),
              coalesce(VVS.Name, '_N/A')
    ) AS SupplierCodeName,

    -- 4. Supplier group code (VendorGroupId or fallback)
    coalesce(nullif(VVS.VendorGroupId, ''), '_N/A') AS SupplierGroupCode,

    -- 5. Supplier group name (description from the look‑up table)
    coalesce(nullif(VVGS.Description, ''), '_N/A') AS SupplierGroupName,

    -- 6. Supplier group code name (group code + ' - ' + group description)
    concat_ws(' - ',
              coalesce(nullif(VVS.VendorGroupId, ''), '_N/A'),
              coalesce(nullif(VVGS.Description, ''), '_N/A')
    ) AS SupplierGroupCodeName,

    -- 7. Address fields – normalise empty strings to '_N/A'
    coalesce(nullif(VVS.FormattedPrimaryAddress, ''), '_N/A') AS Address,
    coalesce(nullif(VVS.AddressZipCode, ''), '_N/A')          AS PostalCode,
    coalesce(nullif(VVS.AddressCity, ''), '_N/A')             AS City,
    coalesce(nullif(VVS.AddressCountryRegionId, ''), '_N/A')
                                                    AS CountryRegionCode,

    -- 8. Company chain name – treat empty string as NULL, then replace
    coalesce(nullif(VVS.CompanyChainName, ''), '_N/A') AS CompanyChainName
FROM `dbe_dbx_internships`.`datastore`.SMRBIVendorStaging VVS
LEFT JOIN `dbe_dbx_internships`.`datastore`.SMRBIVendVendorGroupStaging VVGS
    ON VVS.VendorGroupId     = VVGS.VendorGroupId
   AND VVS.DataAreaId        = VVGS.DataAreaId
;
"""

# COMMAND ----------

# Execute the CREATE VIEW statement
spark.sql(sql)

# COMMAND ----------

# Verify that the view has been created
spark.sql(f"SHOW TABLES IN `dbe_dbx_internships`.`datastore` LIKE 'V_Supplier'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1895)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_Supplier` AS SELECT     -- 1. Vendor table columns     VVS.VendTableRecId        AS SupplierId,     upper(VVS.DataAreaId)     AS CompanyCode,     upper(VVS.VendorAccountNumber) AS SupplierCode,      -- 2. Supplier name, fallback to '_N/A' if NULL or empty     coalesce(VVS.Name, '_N/A') AS SupplierName,      -- 3. Supplier code name (account number + ' - ' + name)     concat_ws(' - ',               upper(VVS.VendorAccountNumber),               coalesce(VVS.Name, '_N/A')     ) AS SupplierCodeName,      -- 4. Supplier group code (VendorGroupId or fallback)     coalesce(nullif(VVS.VendorGroupId, ''), '_N/A') AS SupplierGroupCode,      -- 5. Supplier group name (description from the look‑up table)     coalesce(nullif(VVGS.Description, ''), '_N/A') AS SupplierGroupName,      -- 6. Supplier group code name (group code + ' - ' + group description)     concat_ws(' - ',               coalesce(nullif(VVS.VendorGroupId, ''), '_N/A'),               coalesce(nullif(VVGS.Description, ''), '_N/A')     ) AS SupplierGroupCodeName,      -- 7. Address fields – normalise empty strings to '_N/A'     coalesce(nullif(VVS.FormattedPrimaryAddress, ''), '_N/A') AS Address,     coalesce(nullif(VVS.AddressZipCode, ''), '_N/A')          AS PostalCode,     coalesce(nullif(VVS.AddressCity, ''), '_N/A')             AS City,     coalesce(nullif(VVS.AddressCountryRegionId, ''), '_N/A')                                                     AS CountryRegionCode,      -- 8. Company chain name – treat empty string as NULL, then replace     coalesce(nullif(VVS.CompanyChainName, ''), '_N/A') AS CompanyChainName FROM `_placeholder_`.`_placeholder_`.SMRBIVendorStaging VVS LEFT JOIN `_placeholder_`.`_placeholder_`.SMRBIVendVendorGroupStaging VVGS     ON VVS.VendorGroupId     = VVGS.VendorGroupId    AND VVS.DataAreaId        = VVGS.DataAreaId ;
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
