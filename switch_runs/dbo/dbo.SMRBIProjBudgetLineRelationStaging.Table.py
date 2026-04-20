# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjBudgetLineRelationStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjBudgetLineRelationStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the staging table in the specified catalog & schema
# ------------------------------------------------------------------
# The query is wrapped in an f-string so that the catalog and schema
# values can be injected at runtime.  All data types have been mapped
# from T‑SQL to Spark SQL types.
#
# The SELECT sub‑query returns a single row with NULL values cast to
# the required types so that the resulting table can be created
# immediately and later populated by inserts.
#
# An explicit IF NOT EXISTS check is not required; CREATE OR REPLACE
# guarantees that the table will be materialised in Delta Lake.
#
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjBudgetLineRelationStaging`
USING DELTA
AS
SELECT
    -- 1.  Definition group (string, not null)
    CAST(NULL AS STRING) AS DEFINITIONGROUP,
    -- 2.  Execution ID (string, not null)
    CAST(NULL AS STRING) AS EXECUTIONID,
    -- 3.  Is selected flag (int, not null)
    CAST(NULL AS INT) AS ISSELECTED,
    -- 4.  Transfer status (int, not null)
    CAST(NULL AS INT) AS TRANSFERSTATUS,
    -- 5.  Activity number (string, not null)
    CAST(NULL AS STRING) AS ACTIVITYNUMBER,
    -- 6.  Category ID (string, not null)
    CAST(NULL AS STRING) AS CATEGORYID,
    -- 7.  Committed revisions (decimal(32,6), not null)
    CAST(NULL AS DECIMAL(32,6)) AS COMMITTEDREVISIONS,
    -- 8.  Original budget (decimal(32,6), not null)
    CAST(NULL AS DECIMAL(32,6)) AS ORIGINALBUDGET,
    -- 9.  Projection allocation method (int, not null)
    CAST(NULL AS INT) AS PROJALLOCATIONMETHOD,
    --10.  Projection budget line type (int, not null)
    CAST(NULL AS INT) AS PROJBUDGETLINETYPE,
    --11.  Projection ID (string, not null)
    CAST(NULL AS STRING) AS PROJID,
    --12.  Projection transaction type (int, not null)
    CAST(NULL AS INT) AS PROJTRANSTYPE,
    --13.  Total budget (decimal(32,6), not null)
    CAST(NULL AS DECIMAL(32,6)) AS TOTALBUDGET,
    --14.  Uncommitted revisions (decimal(32,6), not null)
    CAST(NULL AS DECIMAL(32,6)) AS UNCOMMITTEDREVISIONS,
    --15.  Projection budget budget ID (string, not null)
    CAST(NULL AS STRING) AS PROJBUDGET_BUDGETID,
    --16.  Partition key (string, not null)
    CAST(NULL AS STRING) AS PARTITION,
    --17.  Sync start datetime (timestamp, not null)
    CAST(NULL AS TIMESTAMP) AS SYNCSTARTDATETIME
    -- NOTE: Delta Lake does not support a compound PRIMARY KEY like T‑SQL.
    -- The original constraint clause
    --     CONSTRAINT [PK_SMRBIProjBudgetLineRelationStaging] PRIMARY KEY (…) …
    -- is deliberately omitted here.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1966)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIProjBudgetLineRelationStaging` USING DELTA AS SELECT     -- 1.  Definition group (string, not null)     CAST(NULL AS STRING) AS DEFINITIONGROUP,     -- 2.  Execution ID (string, not null)     CAST(NULL AS STRING) AS EXECUTIONID,     -- 3.  Is selected flag (int, not null)     CAST(NULL AS INT) AS ISSELECTED,     -- 4.  Transfer status (int, not null)     CAST(NULL AS INT) AS TRANSFERSTATUS,     -- 5.  Activity number (string, not null)     CAST(NULL AS STRING) AS ACTIVITYNUMBER,     -- 6.  Category ID (string, not null)     CAST(NULL AS STRING) AS CATEGORYID,     -- 7.  Committed revisions (decimal(32,6), not null)     CAST(NULL AS DECIMAL(32,6)) AS COMMITTEDREVISIONS,     -- 8.  Original budget (decimal(32,6), not null)     CAST(NULL AS DECIMAL(32,6)) AS ORIGINALBUDGET,     -- 9.  Projection allocation method (int, not null)     CAST(NULL AS INT) AS PROJALLOCATIONMETHOD,     --10.  Projection budget line type (int, not null)     CAST(NULL AS INT) AS PROJBUDGETLINETYPE,     --11.  Projection ID (string, not null)     CAST(NULL AS STRING) AS PROJID,     --12.  Projection transaction type (int, not null)     CAST(NULL AS INT) AS PROJTRANSTYPE,     --13.  Total budget (decimal(32,6), not null)     CAST(NULL AS DECIMAL(32,6)) AS TOTALBUDGET,     --14.  Uncommitted revisions (decimal(32,6), not null)     CAST(NULL AS DECIMAL(32,6)) AS UNCOMMITTEDREVISIONS,     --15.  Projection budget budget ID (string, not null)     CAST(NULL AS STRING) AS PROJBUDGET_BUDGETID,     --16.  Partition key (string, not null)     CAST(NULL AS STRING) AS PARTITION,     --17.  Sync start datetime (timestamp, not null)     CAST(NULL AS TIMESTAMP) AS SYNCSTARTDATETIME     -- NOTE: Delta Lake does not support a compound PRIMARY KEY like T‑SQL.     -- The original constraint clause     --     CONSTRAINT [PK_SMRBIProjBudgetLineRelationStaging] PRIMARY KEY (…) …     -- is deliberately omitted here.
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
