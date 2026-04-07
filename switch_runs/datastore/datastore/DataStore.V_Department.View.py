# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Department.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Department.View.sql`

# COMMAND ----------

# --------------------------------------------------------------
#  Create a unified view of department dimensions
# --------------------------------------------------------------
# The view “V_Department” combines data from three staging tables
# to produce a distinct list of departments together with a
# combined code/description field.  All objects are fully‑qualified
# in the `dbe_dbx_internships` and `datastore` catalog/namespace that the
# Databricks cluster is configured to use.
#
# Note:
# * `[DataStore].[V_Department]` in T‑SQL translates to
#   `dbe_dbx_internships.datastore.V_Department` in Databricks.
# * Square‑bracketed identifiers are removed – Spark SQL accepts
#   plain identifiers (or optionally back‑ticked strings).
# * The view can be refreshed simply by running this notebook again.
# * The `CONCAT` function replaces the string‑concatenation operator
#   (`+`) used in T‑SQL.
#
# Usage:
#   spark.sql("SELECT * FROM `dbe_dbx_internships`.`datastore`.`V_Department` LIMIT 20")
#
# --------------------------------------------------------------

spark.sql(
"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_Department` AS
SELECT DISTINCT
    DFTS.FinancialTagRecId   AS DepartmentId,
    DFTS.Value              AS DepartmentCode,
    DFTS.Description        AS DepartmentName,
    CONCAT(DFTS.Value, ' ', DFTS.Description) AS DepartmentCodeName,
    DAS.DimensionName      AS DimensionName
FROM   `dbe_dbx_internships`.`datastore`.`SMRBIDimensionFinancialTagStaging` AS DFTS
JOIN   `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeDirCategoryStaging` AS DADCS
       ON DFTS.FinancialTagCategory = DADCS.DirCategory
JOIN   `dbe_dbx_internships`.`datastore`.`SMRBIDimensionAttributeStaging` AS DAS
       ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId
WHERE  DAS.DimensionName = 'Department';
"""
)

# COMMAND ----------

# Example query to verify the view
display(
    spark.sql(
        f"SELECT * FROM `dbe_dbx_internships`.`datastore`.`V_Department` LIMIT 10"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
