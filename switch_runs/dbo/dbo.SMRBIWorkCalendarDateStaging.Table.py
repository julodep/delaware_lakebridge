# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWorkCalendarDateStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWorkCalendarDateStaging.Table.sql`

# COMMAND ----------

# Replace `dbe_dbx_internships` and `dbo` with your Unity Catalog and schema names.
# Note: Avoid using reserved keywords as column names. Here we use PARTITION_COL.

spark.sql(f"""
-- Drop the table if it already exists to avoid errors during repeated runs
DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIWorkCalendarDateStaging`;

-- Create the staging table with equivalent column types
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWorkCalendarDateStaging` (
    DEFINITIONGROUP STRING  NOT NULL,
    EXECUTIONID    STRING  NOT NULL,
    ISSELECTED     INT     NOT NULL,
    TRANSFERSTATUS INT     NOT NULL,
    CALENDARID     STRING  NOT NULL,
    CLOSEDFORPICKUP INT    NOT NULL,
    NAME           STRING  NOT NULL,
    TRANSDATE      TIMESTAMP NOT NULL,
    WORKTIMECONTROL INT    NOT NULL,
    PARTITION_COL  STRING  NOT NULL,
    DATAAREAID     STRING  NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID          BIGINT  NOT NULL
    -- PRIMARY KEY constraint cannot be enforced in Delta Lake; for reference:
    -- PRIMARY KEY (EXECUTIONID, CALENDARID, TRANSDATE, DATAAREAID, PARTITION_COL)
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 957)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN -- Drop the table if it already exists to avoid errors during repeated runs DROP TABLE IF EXISTS `_placeholder_`.`_placeholder_`.`SMRBIWorkCalendarDateStaging`;  -- Create the staging table with equivalent column types CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBIWorkCalendarDateStaging` (     DEFINITIONGROUP STRING  NOT NULL,     EXECUTIONID    STRING  NOT NULL,     ISSELECTED     INT     NOT NULL,     TRANSFERSTATUS INT     NOT NULL,     CALENDARID     STRING  NOT NULL,     CLOSEDFORPICKUP INT    NOT NULL,     NAME           STRING  NOT NULL,     TRANSDATE      TIMESTAMP NOT NULL,     WORKTIMECONTROL INT    NOT NULL,     PARTITION_COL  STRING  NOT NULL,     DATAAREAID     STRING  NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID          BIGINT  NOT NULL     -- PRIMARY KEY constraint cannot be enforced in Delta Lake; for reference:     -- PRIMARY KEY (EXECUTIONID, CALENDARID, TRANSDATE, DATAAREAID, PARTITION_COL) );
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
