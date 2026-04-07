# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_CostCenter.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_CostCenter.View.sql`

# COMMAND ----------

# -----------------------------
#   Translate T‑SQL CREATE VIEW
# -----------------------------
# 1.  Remove `SET` statements and batch separators (`GO`) – they are
#     specific to the legacy SQL Server engine and have no meaning in
#     Databricks.  Their behaviour is documented in the comment.
#
# 2.  Use  `CREATE OR REPLACE VIEW` so that the view can be re‑created
#     in the same notebook without errors.
#
# 3.  All catalogue references are fully‑qualified:  
#     `dbe_dbx_internships`.`datastore`.`object_name`
#
# 4.  String concatenation (`+`) is replaced by Spark's `concat_ws` to
#     avoid `NULL`‑propagation issues.
#
# 5.  The view is created in the :schema `DataStore`.  All tables
#     referenced are assumed to live in the same catalogue/schema
#     provided in the notebook (e.g. `dbe_dbx_internships.datastore`).
#
# 6.  The final statement is executed through `spark.sql`
#     and does not return anything – the view will be available for
#     subsequent queries.

# Note: Replace `dbe_dbx_internships` and `datastore` with the actual names before running.
# Example if catalog = "my_catalog" and schema = "dbo":
#   catalog = "my_catalog"
#   schema = "dbo"

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_CostCenter` AS
SELECT
    DFTS.FinancialTagRecId AS CostCenterId,
    DFTS.Value AS CostCenterCode,
    DFTS.Description AS CostCenterName,
    concat_ws(' ', DFTS.Value, DFTS.Description) AS CostCenterCodeName,
    DAS.DimensionName AS DimensionName
FROM `dbe_dbx_internships`.`datastore`.`SMRBIDImensionFinancialTagStaging` AS DFTS
JOIN `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeDirCategoryStaging` AS DADCS
    ON DFTS.FinancialTagCategory = DADCS.DirCategory
JOIN `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeStaging` AS DAS
    ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId
WHERE DAS.DimensionName = 'CostCenter';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
