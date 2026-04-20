# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDimAttributeVendTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDimAttributeVendTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1. Create the staging table in the target Unity Catalog location
# ------------------------------------------------------------------
# In Spark SQL we use `CREATE OR REPLACE TABLE` (Delta Lake file format by
# default).  All identifiers are fully-qualified: <catalog>.<schema>.<table>.
# ---------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIDimAttributeVendTableStaging` (
    -- Column definitions - NVARCHAR → STRING, INT stays INT, DATETIME → TIMESTAMP
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT     NOT NULL,
    TRANSFERSTATUS INT     NOT NULL,
    NAME           STRING  NOT NULL,
    VALUE          STRING  NOT NULL,
    PARTITION      STRING  NOT NULL,
    DATAAREAID     STRING  NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL

    -- Databricks / Delta Lake does not support a native PRIMARY KEY clause.
    -- See Spark SQL documentation on CHECK constraints or a MERGE/INSERT logic
    -- to enforce uniqueness if required.
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 722)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIDimAttributeVendTableStaging` (     -- Column definitions - NVARCHAR → STRING, INT stays INT, DATETIME → TIMESTAMP     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID    STRING NOT NULL,     ISSELECTED     INT     NOT NULL,     TRANSFERSTATUS INT     NOT NULL,     NAME           STRING  NOT NULL,     VALUE          STRING  NOT NULL,     PARTITION      STRING  NOT NULL,     DATAAREAID     STRING  NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL      -- Databricks / Delta Lake does not support a native PRIMARY KEY clause.     -- See Spark SQL documentation on CHECK constraints or a MERGE/INSERT logic     -- to enforce uniqueness if required. )
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
