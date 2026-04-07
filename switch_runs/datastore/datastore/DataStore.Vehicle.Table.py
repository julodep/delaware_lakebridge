# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Vehicle.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Vehicle.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the Vehicle table in the specified Databricks catalog and schema
# ------------------------------------------------------------------
# Replace the placeholder values with the actual catalog and schema
# names before running the query.
catalog = "<catalog_name>"  # e.g., "my_catalog"
schema  = "<schema_name>"   # e.g., "public"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Vehicle` (
    VehicleId        LONG,          -- BIGINT in T‑SQL
    VehicleCode      STRING,        -- NVARCHAR(30)
    VehicleName      STRING,        -- NVARCHAR(60)
    VehicleCodeName  STRING,        -- NVARCHAR(91)
    DimensionName    STRING         -- NVARCHAR(60)
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 341)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `<catalog_name>`.`<schema_name>`.`Vehicle` (     VehicleId        LONG,          -- BIGINT in T‑SQL     VehicleCode      STRING,        -- NVARCHAR(30)     VehicleName      STRING,        -- NVARCHAR(60)     VehicleCodeName  STRING,        -- NVARCHAR(91)     DimensionName    STRING         -- NVARCHAR(60) )
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
