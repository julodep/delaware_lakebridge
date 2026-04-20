# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.DataManagementExecutionJobDetailStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.DataManagementExecutionJobDetailStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# NOTE: The following Python snippet translates a T‑SQL CREATE TABLE
#       for the staging job detail dataset into a Databricks Delta
#       table. All catalog/schema references are fully‑qualified
#       using the placeholders `dbe_dbx_internships` and `dbo` – replace
#       these with your Unity Catalog catalog and schema names.
#
#       Data types are mapped from SQL Server to Spark SQL:
#           NVARCHAR -> STRING
#           INT      -> INT
#           BIGINT   -> BIGINT
#           DATETIME -> TIMESTAMP
#       The PRIMARY KEY constraint is omitted because Delta Lake
#       does not enforce primary keys natively.  A comment is
#       added to document this decision.
# -------------------------------------------------------------

# Define the fully‑qualified table name
table_name = f"`dbe_dbx_internships`.`dbo`.`DataManagementExecutionJobDetailStaging`"

# COMMAND ----------

# Create the Delta table with the appropriate schema
spark.sql(f"""
CREATE OR REPLACE TABLE {table_name} (
    DEFINITIONGROUP          STRING   NOT NULL,
    EXECUTIONID              STRING   NOT NULL,
    ISSELECTED               INT      NOT NULL,
    TRANSFERSTATUS           INT      NOT NULL,
    DEFINITIONGROUPID        STRING   NOT NULL,
    ENTITYNAME               STRING   NOT NULL,
    JOBID                    STRING   NOT NULL,
    STAGINGENDDATETIME       TIMESTAMP NOT NULL,
    EXCELSHEETNAME            STRING   NOT NULL,
    EXECUTETARGETSTEP         INT      NOT NULL,
    FIRSTROWISHEADER         INT      NOT NULL,
    FILEPATH                 STRING   NOT NULL,
    IGNOREERROR              INT      NOT NULL,
    STAGINGRECORDSTOBEPROCESSEDCOUNT INT NOT NULL,
    PARALLELTASKCOUNT        INT      NOT NULL,
    STAGINGRECORDSCREATEDCOUNT  INT NOT NULL,
    TARGETRECORDSCREATEDCOUNT  INT NOT NULL,
    TARGETRECORDSUPDATEDCOUNT   INT NOT NULL,
    CREATEERRORFILE         INT      NOT NULL,
    RUNBUSINESSLOGIC        INT      NOT NULL,
    RUNBUSINESSVALIDATION   INT      NOT NULL,
    SEQUENCENUMBER          INT      NOT NULL,
    NUMBEROFROWSTOSKIP      INT      NOT NULL,
    SOURCEFORMAT            STRING   NOT NULL,
    STAGINGSTATUS           INT      NOT NULL,
    STAGINGSTARTDATETIME    TIMESTAMP NOT NULL,
    TARGETSTATUS            INT      NOT NULL,
    TARGETENDDATETIME       TIMESTAMP NOT NULL,
    TARGETSTARTDATETIME    TIMESTAMP NOT NULL,
    EXECUTIONUNIT           INT      NOT NULL,
    LEVELINEXECUTIONUNIT    INT      NOT NULL,
    SEQUENCEINLEVEL         INT      NOT NULL,
    FAILEXECUTIONUNITONERROR INT NOT NULL,
    FAILEVELONERROR        INT      NOT NULL,
    PARTITION                STRING  NOT NULL,
    SYNCSTARTDATETIME      TIMESTAMP NOT NULL,
    RECID                   BIGINT  NOT NULL
    -- Note: PRIMARY KEY constraint is not created because Delta Lake
    --       does not support enforcing primary keys.  If you need
    --       uniqueness guarantees, consider adding a UNIQUE index
    --       at query time or handling it in your application logic.
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2134)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`DataManagementExecutionJobDetailStaging` (     DEFINITIONGROUP          STRING   NOT NULL,     EXECUTIONID              STRING   NOT NULL,     ISSELECTED               INT      NOT NULL,     TRANSFERSTATUS           INT      NOT NULL,     DEFINITIONGROUPID        STRING   NOT NULL,     ENTITYNAME               STRING   NOT NULL,     JOBID                    STRING   NOT NULL,     STAGINGENDDATETIME       TIMESTAMP NOT NULL,     EXCELSHEETNAME            STRING   NOT NULL,     EXECUTETARGETSTEP         INT      NOT NULL,     FIRSTROWISHEADER         INT      NOT NULL,     FILEPATH                 STRING   NOT NULL,     IGNOREERROR              INT      NOT NULL,     STAGINGRECORDSTOBEPROCESSEDCOUNT INT NOT NULL,     PARALLELTASKCOUNT        INT      NOT NULL,     STAGINGRECORDSCREATEDCOUNT  INT NOT NULL,     TARGETRECORDSCREATEDCOUNT  INT NOT NULL,     TARGETRECORDSUPDATEDCOUNT   INT NOT NULL,     CREATEERRORFILE         INT      NOT NULL,     RUNBUSINESSLOGIC        INT      NOT NULL,     RUNBUSINESSVALIDATION   INT      NOT NULL,     SEQUENCENUMBER          INT      NOT NULL,     NUMBEROFROWSTOSKIP      INT      NOT NULL,     SOURCEFORMAT            STRING   NOT NULL,     STAGINGSTATUS           INT      NOT NULL,     STAGINGSTARTDATETIME    TIMESTAMP NOT NULL,     TARGETSTATUS            INT      NOT NULL,     TARGETENDDATETIME       TIMESTAMP NOT NULL,     TARGETSTARTDATETIME    TIMESTAMP NOT NULL,     EXECUTIONUNIT           INT      NOT NULL,     LEVELINEXECUTIONUNIT    INT      NOT NULL,     SEQUENCEINLEVEL         INT      NOT NULL,     FAILEXECUTIONUNITONERROR INT NOT NULL,     FAILEVELONERROR        INT      NOT NULL,     PARTITION                STRING  NOT NULL,     SYNCSTARTDATETIME      TIMESTAMP NOT NULL,     RECID                   BIGINT  NOT NULL     -- Note: PRIMARY KEY constraint is not created because Delta Lake     --       does not support enforcing primary keys.  If you need     --       uniqueness guarantees, consider adding a UNIQUE index     --       at query time or handling it in your application logic. );
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
