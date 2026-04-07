# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductionCapacity.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ProductionCapacity.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the `ProductionCapacity` table in Unity Catalog
# ------------------------------------------------------------------
# All identifiers are fully‑qualified as `dbe_dbx_internships`.`datastore`.`ProductionCapacity`.
# Column types are converted from T‑SQL to Spark SQL:
#   • `nvarchar`     → `STRING`
#   • `datetime`     → `TIMESTAMP`
#   • `numeric(p,s)` → `DECIMAL(p,s)`
# The schema is defined exactly as in the original table definition.

# Set your catalog and schema names here
catalog = "<your_catalog>"
schema  = "<your_schema>"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`datastore`.`ProductionCapacity` (
    -- Primary key / identity columns
    ProductionCapacityIdScreening STRING NOT NULL,

    -- Metadata columns
    CompanyCode           STRING NOT NULL,
    PlanVersion           STRING NOT NULL,
    CapacityDate          TIMESTAMP NOT NULL,
    CalendarCode          STRING NOT NULL,
    ResourceCode          STRING NOT NULL,
    RefType               STRING NOT NULL,
    RefCode               STRING NOT NULL,

    -- Capacity values (nullable in the original schema)
    MaximumCapacity       DECIMAL(38,6),
    ReservedCapacity      DECIMAL(38,11),
    AvailableCapacity     DECIMAL(38,17)
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 690)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `<your_catalog>`.`<your_schema>`.`ProductionCapacity` (     -- Primary key / identity columns     ProductionCapacityIdScreening STRING NOT NULL,      -- Metadata columns     CompanyCode           STRING NOT NULL,     PlanVersion           STRING NOT NULL,     CapacityDate          TIMESTAMP NOT NULL,     CalendarCode          STRING NOT NULL,     ResourceCode          STRING NOT NULL,     RefType               STRING NOT NULL,     RefCode               STRING NOT NULL,      -- Capacity values (nullable in the original schema)     MaximumCapacity       DECIMAL(38,6),     ReservedCapacity      DECIMAL(38,11),     AvailableCapacity     DECIMAL(38,17) ) USING DELTA
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
