# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_BudgetModel.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_BudgetModel.View.sql`

# COMMAND ----------

# =============================================================================
#  Create or replace the view V_BudgetModel under the DataStore schema
#  in the target catalog.  
#  All object references are fully‑qualified:
#      dbe_dbx_internships.datastore.SMRBIBudgetModelStaging   – source table (SMRBI staging)
#      dbe_dbx_internships.DataStore.V_BudgetModel           – the view that is created
#  
#  The T‑SQL SET statements (ANSI_NULLS, QUOTED_IDENTIFIER) and the GO
#  batch separators are not required in Spark SQL, so they are omitted.
# =============================================================================

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`DataStore`.`V_BudgetModel` AS
SELECT
    UPPER(bms.DataAreaId)          AS CompanyCode,
    UPPER(bms.BudgetModel)         AS BudgetModelCode,
    CASE WHEN bms.Name = '' THEN '_N/A' ELSE bms.Name END AS BudgetModelName
FROM `dbe_dbx_internships`.`datastore`.`SMRBIBudgetModelStaging` bms
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
