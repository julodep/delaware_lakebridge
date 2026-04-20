# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWrkCtrActivityRequirementSetStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWrkCtrActivityRequirementSetStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the staging table SMRBIWrkCtrActivityRequirementSetStaging
#  in the specified Unity Catalog namespace.  The original T‑SQL
#  definition includes a composite primary key, which Delta Lake does
#  not enforce.  The key definition is preserved in a comment for
#  downstream reference.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWrkCtrActivityRequirementSetStaging` (
    DEFINITIONGROUP   STRING  NOT NULL,
    EXECUTIONID       STRING  NOT NULL,
    ISSELECTED        INT     NOT NULL,
    TRANSFERSTATUS    INT     NOT NULL,
    ACTIVITY          BIGINT  NOT NULL,
    WRKCTRACTIVITYREQUIREMENTSETRECID BIGINT NOT NULL,
    `PARTITION`       STRING  NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID             BIGINT  NOT NULL
    -- Primary key: (EXECUTIONID, WRKCTRACTIVITYREQUIREMENTSETRECID, PARTITION)
    -- Delta Lake does not enforce primary‑key constraints; use this comment for reference.
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 658)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIWrkCtrActivityRequirementSetStaging` (     DEFINITIONGROUP   STRING  NOT NULL,     EXECUTIONID       STRING  NOT NULL,     ISSELECTED        INT     NOT NULL,     TRANSFERSTATUS    INT     NOT NULL,     ACTIVITY          BIGINT  NOT NULL,     WRKCTRACTIVITYREQUIREMENTSETRECID BIGINT NOT NULL,     `PARTITION`       STRING  NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID             BIGINT  NOT NULL     -- Primary key: (EXECUTIONID, WRKCTRACTIVITYREQUIREMENTSETRECID, PARTITION)     -- Delta Lake does not enforce primary‑key constraints; use this comment for reference. )
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
