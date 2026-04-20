# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjBudgetRevisionStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjBudgetRevisionStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1. Create the staging table
# 2. PK clause removed – Delta Lake does not support primary keys
# 3. All column types translated to Databricks standard types
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjBudgetRevisionStaging` (
    DEFINITIONGROUP STRING,            -- NVARCHAR(60)
    EXECUTIONID   STRING,            -- NVARCHAR(90)
    ISSELECTED    INT,               -- INT
    TRANSFERSTATUS INT,              -- INT
    DESCRIPTION   STRING,            -- NVARCHAR(60)
    EXTERNALREFERENCE STRING,         -- NVARCHAR(10)
    REQUESTEDBYCUST STRING,          -- NVARCHAR(20)
    REQUESTEDBYVEND STRING,          -- NVARCHAR(20)
    REQUESTEDDATE TIMESTAMP,         -- DATETIME
    REVISIONDATE  TIMESTAMP,         -- DATETIME
    REVISIONID    STRING,            -- NVARCHAR(10)
    REVISIONWORKFLOWSTATUS INT,      -- INT
    HCMWORKER_PERSONNELNUMBER STRING, -- NVARCHAR(25)
    DIRPERSON_FK_PARTYNUMBER STRING,   -- NVARCHAR(40)
    PROJBUDGET_BUDGETID STRING,        -- NVARCHAR(10)
    PARTITION       STRING,            -- NVARCHAR(20)
    DATAAREAID      STRING,            -- NVARCHAR(4)
    SYNCSTARTDATETIME TIMESTAMP,       -- DATETIME
    RECID           BIGINT            -- BIGINT
) USING DELTA          -- Delta Lake table (recommended for Databricks)
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 4. Optional: Verify the table schema
# ------------------------------------------------------------------
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIProjBudgetRevisionStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1146)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIProjBudgetRevisionStaging` (     DEFINITIONGROUP STRING,            -- NVARCHAR(60)     EXECUTIONID   STRING,            -- NVARCHAR(90)     ISSELECTED    INT,               -- INT     TRANSFERSTATUS INT,              -- INT     DESCRIPTION   STRING,            -- NVARCHAR(60)     EXTERNALREFERENCE STRING,         -- NVARCHAR(10)     REQUESTEDBYCUST STRING,          -- NVARCHAR(20)     REQUESTEDBYVEND STRING,          -- NVARCHAR(20)     REQUESTEDDATE TIMESTAMP,         -- DATETIME     REVISIONDATE  TIMESTAMP,         -- DATETIME     REVISIONID    STRING,            -- NVARCHAR(10)     REVISIONWORKFLOWSTATUS INT,      -- INT     HCMWORKER_PERSONNELNUMBER STRING, -- NVARCHAR(25)     DIRPERSON_FK_PARTYNUMBER STRING,   -- NVARCHAR(40)     PROJBUDGET_BUDGETID STRING,        -- NVARCHAR(10)     PARTITION       STRING,            -- NVARCHAR(20)     DATAAREAID      STRING,            -- NVARCHAR(4)     SYNCSTARTDATETIME TIMESTAMP,       -- DATETIME     RECID           BIGINT            -- BIGINT ) USING DELTA          -- Delta Lake table (recommended for Databricks)
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
