# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventTableModuleStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventTableModuleStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook – Create table `SMRBIInventTableModuleStaging`
# Target catalog and schema: `dbe_dbx_internships` and `dbo`
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventTableModuleStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ITEMID STRING NOT NULL,
    MODULETYPE INT NOT NULL,
    UNITID STRING NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
    -- NOTE: The original T‑SQL defined a PRIMARY KEY cluster.
    -- Delta Lake does not enforce primary keys; we omit the constraint
    -- but keep the column definitions unchanged.  If uniqueness is
    -- required, consider adding a unique index or enforcing logic
    -- in application code or streaming pipelines.
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 770)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIInventTableModuleStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     ITEMID STRING NOT NULL,     MODULETYPE INT NOT NULL,     UNITID STRING NOT NULL,     COMPANY STRING NOT NULL,     PARTITION STRING NOT NULL,     DATAAREAID STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL     -- NOTE: The original T‑SQL defined a PRIMARY KEY cluster.     -- Delta Lake does not enforce primary keys; we omit the constraint     -- but keep the column definitions unchanged.  If uniqueness is     -- required, consider adding a unique index or enforcing logic     -- in application code or streaming pipelines. )
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
