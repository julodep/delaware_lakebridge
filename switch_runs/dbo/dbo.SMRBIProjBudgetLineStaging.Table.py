# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjBudgetLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjBudgetLineStaging.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------------------
# Create the staging table in the target catalog/schema.
# Each column is mapped to a Spark SQL data type.
# Delta Lake does not support a traditional PRIMARY KEY constraint,
# so we list the key columns in the comment for reference.
# ----------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjBudgetLineStaging` (
    DEFINITIONGROUP          STRING  NOT NULL,
    EXECUTIONID              STRING  NOT NULL,
    ISSELECTED               INT     NOT NULL,
    TRANSFERSTATUS           INT     NOT NULL,
    ACTIVITYNUMBER           STRING  NOT NULL,
    CATEGORYID               STRING  NOT NULL,
    COMMITTEDREVISIONS       DECIMAL(32,6) NOT NULL,
    ORIGINALBUDGET           DECIMAL(32,6) NOT NULL,
    PROJALLOCATIONMETHOD     INT     NOT NULL,
    PROJBUDGETLINETYPE       INT     NOT NULL,
    PROJID                   STRING  NOT NULL,
    PROJTRANSTYPE            INT     NOT NULL,
    TOTALBUDGET              DECIMAL(32,6) NOT NULL,
    UNCOMMITTEDREVISIONS     DECIMAL(32,6) NOT NULL,
    PROJBUDGET_BUDGETID      STRING  NOT NULL,
    PARTITION                STRING  NOT NULL,
    DATAAREAID               STRING  NOT NULL,
    SYNCSTARTDATETIME        TIMESTAMP NOT NULL,
    RECID                    BIGINT  NOT NULL,
    -- Composite primary key (for reference – not enforced by Delta):
    -- EXECUTIONID, ACTIVITYNUMBER, CATEGORYID, PROJBUDGETLINETYPE,
    -- PROJID, PROJBUDGET_BUDGETID, DATAAREAID, PARTITION
    COMMENT 'Composite key: EXECUTIONID + ACTIVITYNUMBER + CATEGORYID + PROJBUDGETLINETYPE + PROJID + PROJBUDGET_BUDGETID + DATAAREAID + PARTITION'
) USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1371)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIProjBudgetLineStaging` (     DEFINITIONGROUP          STRING  NOT NULL,     EXECUTIONID              STRING  NOT NULL,     ISSELECTED               INT     NOT NULL,     TRANSFERSTATUS           INT     NOT NULL,     ACTIVITYNUMBER           STRING  NOT NULL,     CATEGORYID               STRING  NOT NULL,     COMMITTEDREVISIONS       DECIMAL(32,6) NOT NULL,     ORIGINALBUDGET           DECIMAL(32,6) NOT NULL,     PROJALLOCATIONMETHOD     INT     NOT NULL,     PROJBUDGETLINETYPE       INT     NOT NULL,     PROJID                   STRING  NOT NULL,     PROJTRANSTYPE            INT     NOT NULL,     TOTALBUDGET              DECIMAL(32,6) NOT NULL,     UNCOMMITTEDREVISIONS     DECIMAL(32,6) NOT NULL,     PROJBUDGET_BUDGETID      STRING  NOT NULL,     PARTITION                STRING  NOT NULL,     DATAAREAID               STRING  NOT NULL,     SYNCSTARTDATETIME        TIMESTAMP NOT NULL,     RECID                    BIGINT  NOT NULL,     -- Composite primary key (for reference – not enforced by Delta):     -- EXECUTIONID, ACTIVITYNUMBER, CATEGORYID, PROJBUDGETLINETYPE,     -- PROJID, PROJBUDGET_BUDGETID, DATAAREAID, PARTITION     COMMENT 'Composite key: EXECUTIONID + ACTIVITYNUMBER + CATEGORYID + PROJBUDGETLINETYPE + PROJID + PROJBUDGET_BUDGETID + DATAAREAID + PARTITION' ) USING delta
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
