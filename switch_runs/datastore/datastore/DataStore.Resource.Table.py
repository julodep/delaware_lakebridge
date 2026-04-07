# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Resource.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Resource.Table.sql`

# COMMAND ----------

# Create the table DataStore.Resource in the target catalog and schema.
# All references are fully‑qualified with backticks to avoid issues with
# special characters or reserved words.
# Data types are mapped from T‑SQL to Spark SQL following the rules above.
# The table is created as a Delta table (the default for Databricks).

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Resource` (
    -- Primary key / unique constraints are not specified in the T‑SQL, so
    -- we simply create the columns as shown.
    ResourceCode        STRING NOT NULL,
    ResourceName        STRING NOT NULL,
    ResourceCodeName    STRING,
    ResourceGroupCode   STRING NOT NULL,
    ResourceGroupName   STRING NOT NULL,
    ResourceGroupCodeName STRING,
    CompanyCode        STRING NOT NULL,
    ResourceType       STRING NOT NULL,
    InputWarehouseCode STRING NOT NULL,
    InputWarehouseLocationCode STRING NOT NULL,
    OutputWarehouseCode STRING NOT NULL,
    OutputWarehouseLocationCode STRING NOT NULL,
    EfficiencyPercentage DECIMAL(32,6),
    RouteGroupCode     STRING NOT NULL,
    HasFiniteSchedulingCapacity INT,
    ValidFromDate      TIMESTAMP,
    ValidToDate        TIMESTAMP,
    CalendarCode       STRING NOT NULL
)
USING DELTA;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 924)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`Resource` (     -- Primary key / unique constraints are not specified in the T‑SQL, so     -- we simply create the columns as shown.     ResourceCode        STRING NOT NULL,     ResourceName        STRING NOT NULL,     ResourceCodeName    STRING,     ResourceGroupCode   STRING NOT NULL,     ResourceGroupName   STRING NOT NULL,     ResourceGroupCodeName STRING,     CompanyCode        STRING NOT NULL,     ResourceType       STRING NOT NULL,     InputWarehouseCode STRING NOT NULL,     InputWarehouseLocationCode STRING NOT NULL,     OutputWarehouseCode STRING NOT NULL,     OutputWarehouseLocationCode STRING NOT NULL,     EfficiencyPercentage DECIMAL(32,6),     RouteGroupCode     STRING NOT NULL,     HasFiniteSchedulingCapacity INT,     ValidFromDate      TIMESTAMP,     ValidToDate        TIMESTAMP,     CalendarCode       STRING NOT NULL ) USING DELTA;
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
