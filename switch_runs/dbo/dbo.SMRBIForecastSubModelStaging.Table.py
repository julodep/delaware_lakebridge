# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIForecastSubModelStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIForecastSubModelStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Create the staging table `SMRBIForecastSubModelStaging`
# ------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIForecastSubModelStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID     STRING NOT NULL,
    ISSELECTED      INT    NOT NULL,
    TRANSFERSTATUS  INT    NOT NULL,
    MODELID         STRING NOT NULL,
    SUBMODELID      STRING NOT NULL,
    PARTITION       STRING NOT NULL,
    DATAAREAID      STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
    -- Primary key constraint is omitted because Delta Lake does not
    -- support a declarative PRIMARY KEY. This table can be queried
    -- with the usual Delta optimizations; if a uniqueness guarantee
    -- is required, enforce it at the application level or via
    -- an additional external index.
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------
# OPTIONAL: Inspect the schema to confirm correct types
# ------------------------------------------------------------
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIForecastSubModelStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 756)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `_placeholder_`.`_placeholder_`.`SMRBIForecastSubModelStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID     STRING NOT NULL,     ISSELECTED      INT    NOT NULL,     TRANSFERSTATUS  INT    NOT NULL,     MODELID         STRING NOT NULL,     SUBMODELID      STRING NOT NULL,     PARTITION       STRING NOT NULL,     DATAAREAID      STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL     -- Primary key constraint is omitted because Delta Lake does not     -- support a declarative PRIMARY KEY. This table can be queried     -- with the usual Delta optimizations; if a uniqueness guarantee     -- is required, enforce it at the application level or via     -- an additional external index. ) USING DELTA
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
