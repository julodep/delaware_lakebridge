# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjActivityStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjActivityStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the persistent table `SMRBIProjActivityStaging` in the target
# catalog and schema.  The T‑SQL column types are translated as follows:
#   NVARCHAR → STRING
#   INT      → INT
#   DATETIME → TIMESTAMP
# ------------------------------------------------------------------

# Note: Replace `dbe_dbx_internships` and `dbo` with the actual catalog and
# schema names before running the notebook.  All references are fully
# qualified, e.g. `dbe_dbx_internships.dbo.SMRBIProjActivityStaging`.

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjActivityStaging` (
        -- NVARCHAR columns are mapped to STRING
        DEFINITIONGROUP STRING NOT NULL,
        EXECUTIONID STRING NOT NULL,
        ISSELECTED INT NOT NULL,
        TRANSFERSTATUS INT NOT NULL,
        ACTIVITYNUMBER STRING NOT NULL,
        COMPANY STRING NOT NULL,
        TXT STRING NOT NULL,
        `PARTITION` STRING NOT NULL,
        DATAAREAID STRING NOT NULL,
        -- DATETIME maps to TIMESTAMP
        SYNCSTARTDATETIME TIMESTAMP NOT NULL
    )
    USING DELTA
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 569)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIProjActivityStaging` (         -- NVARCHAR columns are mapped to STRING         DEFINITIONGROUP STRING NOT NULL,         EXECUTIONID STRING NOT NULL,         ISSELECTED INT NOT NULL,         TRANSFERSTATUS INT NOT NULL,         ACTIVITYNUMBER STRING NOT NULL,         COMPANY STRING NOT NULL,         TXT STRING NOT NULL,         `PARTITION` STRING NOT NULL,         DATAAREAID STRING NOT NULL,         -- DATETIME maps to TIMESTAMP         SYNCSTARTDATETIME TIMESTAMP NOT NULL     )     USING DELTA
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
