# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Currency.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Currency.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------------------
#  Create the table `Currency` in the target Unity Catalog location
# ----------------------------------------------------------------------
#  In T‑SQL the table was defined in the `DataStore` schema with
#  `[CurrencyCode] [nvarchar](3) NULL` and `[CurrencyName] [nvarchar](60) NOT NULL`.
#  In Spark SQL the equivalent data types are `STRING`.  
#  `NVARCHAR` → `STRING`, the length specifier is ignored in Delta Lake.
#  The `NOT NULL` constraint is preserved, while the `NULL` qualifier is
#  omitted because columns are nullable by default.
#  The `ON [PRIMARY]` clause is a SQL Server storage hint – it has no
#  equivalent in Databricks and is therefore omitted.
#  The `SET ANSI_NULLS` / `SET QUOTED_IDENTIFIER` statements are session
#  settings that do not affect the table definition in Spark, so they are
#  not translated.
# ----------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Currency` (
    CurrencyCode STRING,
    CurrencyName STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
