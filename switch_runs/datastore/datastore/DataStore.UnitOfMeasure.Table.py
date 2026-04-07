# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.UnitOfMeasure.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.UnitOfMeasure.Table.sql`

# COMMAND ----------

# ----------------------------------------------------
# Databricks notebook – Create DataStore.UnitOfMeasure
# ----------------------------------------------------
# This notebook translates the T‑SQL CREATE TABLE statement into a Spark
# SQL statement that works with Unity Catalog.  All identifiers are fully
# qualified as:
#   dbe_dbx_internships.datastore.UnitOfMeasure
#
# Data type mapping:
#   INT        -> INT
#   NVARCHAR(n)-> STRING
#   NUMERIC(p,s) -> DECIMAL(p,s)
#
# Drop the table if it already exists to prevent “table already exists”
# errors, then create it with the exact column list and types.
#
# ----------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE
  `dbe_dbx_internships`.`datastore`.`UnitOfMeasure` (
    Denominator     INT          NOT NULL,
    DataFlow        STRING,
    Factor          DECIMAL(38,21),
    InnerOffset     DECIMAL(38,12) NOT NULL,
    OuterOffset     DECIMAL(38,12) NOT NULL,
    Product         STRING       NOT NULL,
    ItemNumber      STRING       NOT NULL,
    Rounding        INT          NOT NULL,
    FromUOM         STRING,
    ToUOM           STRING,
    CompanyCode     STRING
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
