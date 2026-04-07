# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_EndCustomer.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_EndCustomer.View.sql`

# COMMAND ----------

# Create or replace the persistent view for EndCustomer analytic data
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_EndCustomer` AS
SELECT DISTINCT
    DFTS.FinancialTagRecId   AS EndCustomerId,
    DFTS.Value               AS EndCustomerCode,
    DFTS.Description         AS EndCustomerName,
    concat_ws(' ', DFTS.Value, DFTS.Description) AS EndCustomerCodeName,
    DAS.DimensionName        AS DimensionName
FROM `dbe_dbx_internships`.`datastore`.`SMRBIDimensionFinancialTagStaging` DFTS
JOIN `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeDirCategoryStaging` DADCS
    ON DFTS.FinancialTagCategory = DADCS.DirCategory
JOIN `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeStaging` DAS
    ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId
WHERE DAS.DimensionName = 'Endcustomer';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
