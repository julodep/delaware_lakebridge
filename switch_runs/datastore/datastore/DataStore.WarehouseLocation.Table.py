# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.WarehouseLocation.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.WarehouseLocation.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks Notebook – Convert T‑SQL CREATE TABLE to Delta Lake
#
# This script translates the following T‑SQL:
#
#   CREATE TABLE [DataStore].[WarehouseLocation] (
#       [RecId] BIGINT NOT NULL,
#       [CompanyCode] NVARCHAR(4) NULL,
#       [WarehouseLocationCode] NVARCHAR(10) NOT NULL,
#       [WarehouseCode] NVARCHAR(10) NULL,
#       [WarehouseName] NVARCHAR(60) NOT NULL,
#       [WarehouseCodeName] NVARCHAR(73) NULL,
#       [WareHouseLocationType] NVARCHAR(20) NOT NULL
#   );
#
# The fully‑qualified table name in Databricks will be:
#       `dbe_dbx_internships`.`datastore`.`WarehouseLocation`
#
# Mapping of T‑SQL types to Spark SQL types:
#       BIGINT      → LONG          (most 64‑bit integer type)
#       NVARCHAR → STRING          (Spark has no native length metadata)
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`WarehouseLocation` (
    RecId                     LONG,   -- BIGINT → LONG
    CompanyCode                STRING, -- NVARCHAR(4) → STRING
    WarehouseLocationCode     STRING, -- NVARCHAR(10) → STRING
    WarehouseCode              STRING, -- NVARCHAR(10) → STRING
    WarehouseName              STRING, -- NVARCHAR(60) → STRING
    WarehouseCodeName          STRING, -- NVARCHAR(73) → STRING
    WareHouseLocationType      STRING  -- NVARCHAR(20) → STRING
)
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Validate that the table was created successfully
# ------------------------------------------------------------------
df = spark.table(f"dbe_dbx_internships.datastore.WarehouseLocation")
display(df.limit(5))   # Show first 5 rows (will be empty until data is inserted)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 524)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`WarehouseLocation` (     RecId                     LONG,   -- BIGINT → LONG     CompanyCode                STRING, -- NVARCHAR(4) → STRING     WarehouseLocationCode     STRING, -- NVARCHAR(10) → STRING     WarehouseCode              STRING, -- NVARCHAR(10) → STRING     WarehouseName              STRING, -- NVARCHAR(60) → STRING     WarehouseCodeName          STRING, -- NVARCHAR(73) → STRING     WareHouseLocationType      STRING  -- NVARCHAR(20) → STRING )
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
