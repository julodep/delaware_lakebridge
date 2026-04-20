# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBILedgerStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBILedgerStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# 1️⃣  Create the SMRBILedgerStaging table in Delta Lake
# --------------------------------------------------------------
# Set defaults for catalog and schema if not already defined
catalog = locals().get('catalog', 'default')
schema = locals().get('schema', 'public')

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBILedgerStaging` (
    DEFINITIONGROUP          STRING,   -- NVARCHAR(60)
    EXECUTIONID              STRING,   -- NVARCHAR(90)
    ISSELECTED               INT,      -- INT
    TRANSFERSTATUS           INT,      -- INT
    BUDGETEXCHANGERATETYPE    STRING,   -- NVARCHAR(20)
    ACCOUNTINGCURRENCY        STRING,   -- NVARCHAR(3)
    CHARTOFACCOUNTSRECID     BIGINT,   -- BIGINT
    EXCHANGERATETYPE          STRING,   -- NVARCHAR(20)
    NAME                      STRING,   -- NVARCHAR(20)
    REPORTINGCURRENCY        STRING,   -- NVARCHAR(3)
    LEDGERRECID              BIGINT,   -- BIGINT
    PARTITION                STRING,   -- NVARCHAR(20)
    SYNCSTARTDATETIME        TIMESTAMP, -- DATETIME
    RECID                     BIGINT    -- BIGINT
)
PARTITIONED BY (EXECUTIONID, LEDGERRECID, PARTITION)
""")

# COMMAND ----------

# --------------------------------------------------------------
# 2️⃣  Verify the schema of the created table (optional)
# --------------------------------------------------------------
metadata = spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBILedgerStaging`")
display(metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 875)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBILedgerStaging` (     DEFINITIONGROUP          STRING,   -- NVARCHAR(60)     EXECUTIONID              STRING,   -- NVARCHAR(90)     ISSELECTED               INT,      -- INT     TRANSFERSTATUS           INT,      -- INT     BUDGETEXCHANGERATETYPE    STRING,   -- NVARCHAR(20)     ACCOUNTINGCURRENCY        STRING,   -- NVARCHAR(3)     CHARTOFACCOUNTSRECID     BIGINT,   -- BIGINT     EXCHANGERATETYPE          STRING,   -- NVARCHAR(20)     NAME                      STRING,   -- NVARCHAR(20)     REPORTINGCURRENCY        STRING,   -- NVARCHAR(3)     LEDGERRECID              BIGINT,   -- BIGINT     PARTITION                STRING,   -- NVARCHAR(20)     SYNCSTARTDATETIME        TIMESTAMP, -- DATETIME     RECID                     BIGINT    -- BIGINT ) PARTITIONED BY (EXECUTIONID, LEDGERRECID, PARTITION)
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
