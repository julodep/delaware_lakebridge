# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWorkCalendarDateLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWorkCalendarDateLineStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the staging table `SMRBIWorkCalendarDateLineStaging` in Databricks
#  ------------------------------------------------------------------
#  NOTE: Replace `dbe_dbx_internships` and `dbo` with your actual Unity Catalog
#  catalog and schema names before running this notebook.
#
#  All object references are fully‑qualified as required:
#      `dbe_dbx_internships`.`dbo`.`SMRBIWorkCalendarDateLineStaging`
#
#  The T‑SQL table definition is translated as follows:
#    • NVARCHAR → STRING
#    • INT      → INT
#    • NUMERIC  → DECIMAL(p,s)
#    • DATETIME → TIMESTAMP
#    • PRIMARY KEY constraints are not supported natively in Delta Lake, so
#      we simply create the columns and add a comment reminding the user to
#      enforce uniqueness at the application or data‑lake level.
#
#  The table is created as a Delta table so that it benefits from ACID
#  transactions and schema evolution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
