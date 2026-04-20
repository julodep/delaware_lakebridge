# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICostSheetNodeTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICostSheetNodeTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# 1️⃣  Create a staging table in the specified catalog & schema
# ------------------------------------------------------------
#  - All identifiers are fully‑qualified (`dbe_dbx_internships`.`dbo`.`SMRBICostSheetNodeTableStaging`).
#  - T‑SQL data types are mapped to Spark SQL types:
#        NVARCHAR -> STRING
#        INT      -> INT
#        DATETIME -> TIMESTAMP
#  - The PRIMARY KEY clause is omitted because Delta Lake (Spark) does not enforce
#    primary‑key constraints.  The clause is left in a comment for reference.
# ------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICostSheetNodeTableStaging` (
    -- 60‑character string: definition group
    DEFINITIONGROUP STRING,
    -- 90‑character string: execution identifier
    EXECUTIONID STRING,
    ISSELECTED INT,
    TRANSFERSTATUS INT,
    CODE STRING,
    COSTGROUPID STRING,
    COMPANY STRING,
    DESCRIPTION STRING,
    -- level is reserved in Python, so keep the trailing underscore
    LEVEL_ INT,
    NODEID INT,
    PARENTNODEID INT,
    PARTITION STRING,
    DATAAREAID STRING,
    SYNCSTARTDATETIME TIMESTAMP
);
-- PRIMARY KEY (EXECUTIONID, NODEID, DATAAREAID, PARTITION) NOT SUPPORTED in Spark
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
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBICostSheetNodeTableStaging` (     -- 60‑character string: definition group     DEFINITIONGROUP STRING,     -- 90‑character string: execution identifier     EXECUTIONID STRING,     ISSELECTED INT,     TRANSFERSTATUS INT,     CODE STRING,     COSTGROUPID STRING,     COMPANY STRING,     DESCRIPTION STRING,     -- level is reserved in Python, so keep the trailing underscore     LEVEL_ INT,     NODEID INT,     PARENTNODEID INT,     PARTITION STRING,     DATAAREAID STRING,     SYNCSTARTDATETIME TIMESTAMP ); -- PRIMARY KEY (EXECUTIONID, NODEID, DATAAREAID, PARTITION) NOT SUPPORTED in Spark
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
