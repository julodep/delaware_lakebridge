# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.GroupCurrency.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324104824-3bci/ETL.GroupCurrency.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: Import any required libraries (Databricks provides Spark by default)
# ------------------------------------------------------------

# ------------------------------------------------------------
# T-SQL session settings (ANSI_NULLS, QUOTED_IDENTIFIER) are not applicable in Databricks.
# They are omitted. The following CREATE TABLE statement is adapted for Spark SQL.
# ------------------------------------------------------------

# Create the GroupCurrency table in the target catalog and schema.
# The original schema [ETL] is mapped to the required `switchschema` schema.
# NVARCHAR maps to STRING in Spark SQL. Table options like ON [PRIMARY] are not supported and are omitted.
spark.sql("""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.switchschema.GroupCurrency (
    GroupCurrencyCode STRING NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
