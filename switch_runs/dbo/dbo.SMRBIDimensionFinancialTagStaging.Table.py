# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDimensionFinancialTagStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDimensionFinancialTagStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Databricks notebook:  Create the staging table
# Target catalog and schema: dbe_dbx_internships.dbo
#
# The original T‑SQL statement defines a table with a primary‑key constraint;
# Delta Lake (Spark SQL) does not support PRIMARY KEY declaration, so we
# create the table with the requested columns only.
# -------------------------------------------------------------

# Drop the table if it already exists to keep the example idempotent
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIDimensionFinancialTagStaging`")

# COMMAND ----------

# Create the table with data types mapped to Spark SQL:
#     nvarchar -> STRING   ,  datetime -> TIMESTAMP
# -------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIDimensionFinancialTagStaging` (
    DEFINITIONGROUP STRING NOT NULL,      -- nvarchar(60)
    EXECUTIONID   STRING NOT NULL,        -- nvarchar(90)
    ISSELECTED    INT NOT NULL,          -- int
    TRANSFERSTATUS INT NOT NULL,         -- int
    DESCRIPTION   STRING NOT NULL,        -- nvarchar(60)
    FINANCIALTAGCATEGORY BIGINT NOT NULL,-- bigint
    VALUE          STRING NOT NULL,      -- nvarchar(30)
    FINANCIALTAGRECID BIGINT NOT NULL,   -- bigint
    PARTITION      STRING NOT NULL,      -- nvarchar(20)
    SYNCSTARTDATETIME TIMESTAMP NOT NULL, -- datetime -> timestamp
    RECID          BIGINT NOT NULL       -- bigint
)
USING delta
""")

# COMMAND ----------

# -------------------------------------------------------------
# Verify the table was created
spark.sql(f"SHOW TABLES IN `dbe_dbx_internships`.`dbo`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 719)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIDimensionFinancialTagStaging` (     DEFINITIONGROUP STRING NOT NULL,      -- nvarchar(60)     EXECUTIONID   STRING NOT NULL,        -- nvarchar(90)     ISSELECTED    INT NOT NULL,          -- int     TRANSFERSTATUS INT NOT NULL,         -- int     DESCRIPTION   STRING NOT NULL,        -- nvarchar(60)     FINANCIALTAGCATEGORY BIGINT NOT NULL,-- bigint     VALUE          STRING NOT NULL,      -- nvarchar(30)     FINANCIALTAGRECID BIGINT NOT NULL,   -- bigint     PARTITION      STRING NOT NULL,      -- nvarchar(20)     SYNCSTARTDATETIME TIMESTAMP NOT NULL, -- datetime -> timestamp     RECID          BIGINT NOT NULL       -- bigint ) USING delta
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
