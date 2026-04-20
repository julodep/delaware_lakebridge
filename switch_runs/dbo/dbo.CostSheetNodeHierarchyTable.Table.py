# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.CostSheetNodeHierarchyTable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.CostSheetNodeHierarchyTable.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------------------------
# Create a persistent Delta table that matches the T‑SQL definition of
# [dbo].[CostSheetNodeHierarchyTable].
#
# The original definition uses NVARCHAR, INT and NULL/NOT NULL qualifiers.
# In Spark SQL the equivalent data types are:
#   NVARCHAR   → STRING          (Spark does not enforce the length, but the data type is STRING)
#   INT        → INT
#   NULL/NOT NULL  → The default in Spark is nullable; explicit NULL is not required.
#
# We drop the table if it already exists to make the script idempotent.
# The table is created as a Delta Lake table in the specified catalog and schema.
# ------------------------------------------------------------------------------------

# Fully‑qualified table name (ensure each component is quoted)
FULL_TABLE = f"`dbe_dbx_internships`.`dbo`.`CostSheetNodeHierarchyTable`"

# COMMAND ----------

# 1. Drop the table if it already exists (idempotent)
spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

# COMMAND ----------

# 2. Create the table with the same column set
spark.sql(f"""
  CREATE TABLE {FULL_TABLE} (
    DataAreaId STRING,
    AccId INT,
    Level_1_Code STRING,
    Level_1_Description STRING,
    Level_1_CostGroupId STRING,
    Level_2_Code STRING,
    Level_2_Description STRING,
    Level_2_CostGroupId STRING,
    Level_3_Code STRING,
    Level_3_Description STRING,
    Level_3_CostGroupId STRING
  )
  USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
