# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductConfiguration.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ProductConfiguration.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
#  Create the `ProductConfiguration` table in Unity Catalog
#  (catalog – your Databricks catalog name, schema – your schema name)
# -------------------------------------------------------------
# T‑SQL data‑type mapping notes:
#   nvarchar → STRING (Spark SQL VARCHAR/STRING)
#   NULL / NOT NULL → Spark columns are nullable by default; `NOT NULL`
#                     can be added if needed (Delta Lake supports it).
#
# The CREATE TABLE statement below uses fully‑qualified naming:
#   `dbe_dbx_internships`.`datastore`.`ProductConfiguration`
#
# -------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`datastore`.`ProductConfiguration`
(
    CompanyCode STRING,                -- nullable
    InventDimCode STRING NOT NULL,
    ProductConfigurationCode STRING NOT NULL,
    InventBatchCode STRING NOT NULL,
    InventColorCode STRING NOT NULL,
    InventSizeCode STRING NOT NULL,
    InventStyleCode STRING NOT NULL,
    InventStatusCode STRING NOT NULL,
    SiteCode STRING NOT NULL,
    SiteName STRING NOT NULL,
    WarehouseCode STRING NOT NULL,
    WarehouseName STRING NOT NULL,
    WarehouseLocationCode STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 582)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `_placeholder_`.`_placeholder_`.`ProductConfiguration` (     CompanyCode STRING,                -- nullable     InventDimCode STRING NOT NULL,     ProductConfigurationCode STRING NOT NULL,     InventBatchCode STRING NOT NULL,     InventColorCode STRING NOT NULL,     InventSizeCode STRING NOT NULL,     InventStyleCode STRING NOT NULL,     InventStatusCode STRING NOT NULL,     SiteCode STRING NOT NULL,     SiteName STRING NOT NULL,     WarehouseCode STRING NOT NULL,     WarehouseName STRING NOT NULL,     WarehouseLocationCode STRING NOT NULL )
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
