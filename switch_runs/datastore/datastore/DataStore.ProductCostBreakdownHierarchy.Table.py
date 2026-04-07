# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductCostBreakdownHierarchy.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ProductCostBreakdownHierarchy.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Create the ProductCostBreakdownHierarchy table in Databricks
# -------------------------------------------------------------
# Each column was translated from the T‑SQL definition:
#   NVARCHAR(n)          → STRING  (Spark SQL data type)
#   NULL / NOT NULL     →  kept as is
#
# The table is created under the catalog and schema that are
# provided as placeholders `dbe_dbx_internships` and `datastore`.  Replace
# those when you run the notebook in your workspace.
#
# Example:
#   spark.sql(f\"\"\"
#   CREATE TABLE `my_catalog`.`my_schema`.`ProductCostBreakdownHierarchy` (
#     ...
#   ) USING DELTA
#   \"\"\")
#
# For simplicity the table is created using the default Delta
# engine – you can adjust the `USING DELTA` clause if you
# prefer another storage format.

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`datastore`.`ProductCostBreakdownHierarchy` (
  CompanyCode STRING,
  Level_1 STRING NOT NULL,
  Level_2 STRING NOT NULL,
  Level_3 STRING NOT NULL
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
