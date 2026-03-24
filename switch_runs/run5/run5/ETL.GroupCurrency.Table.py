# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.GroupCurrency.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324101927-oqdi/ETL.GroupCurrency.Table.sql`

# COMMAND ----------

# ==============================
# Create the GroupCurrency table
# NOTE: The original T‑SQL statements `SET ANSI_NULLS ON` and `SET QUOTED_IDENTIFIER ON`
# are session settings that have no effect in Databricks/Spark SQL, so they are omitted.
# The `ON [PRIMARY]` clause is specific to SQL Server storage; it is not needed here.
# The table is created in the target catalog `dbe_dbx_internships` and schema `switchschema`.
# ==============================

spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.GroupCurrency (
    GroupCurrencyCode STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
