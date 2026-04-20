# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventItemBatchStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventItemBatchStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# This notebook creates the staging table `SMRBIInventItemBatchStaging`
# in the specified Unity Catalog database `dbe_dbx_internships`.`dbo`.
# ------------------------------------------------------------
# 1. Drop the table if it already exists to avoid conflicts
#    (the `CREATE OR REPLACE` statement will overwrite it, but the
#     explicit drop improves idempotency for users that may want
#     to clean the table manually).
# ------------------------------------------------------------
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIInventItemBatchStaging`")

# COMMAND ----------

# ------------------------------------------------------------
# 2. Create the table using Spark SQL (Delta Lake).
#    All column names and data types are mapped from the original
#    T‑SQL types to Spark SQL equivalents:
#      - NVARCHAR → STRING
#      - INT      → INT
#      - BIGINT   → BIGINT (Spark uses BIGINT)
#      - DATETIME → TIMESTAMP
#      - NVARCHAR(max) → STRING (large text is stored as string)
#    Primary keys are not enforced in Delta Lake; we comment them
#    out but list the intended key columns for documentation
#    purposes.
# ------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventItemBatchStaging` (
    DEFINITIONGROUP    STRING   NOT NULL,   -- nvarchar(60)
    EXECUTIONID        STRING   NOT NULL,   -- nvarchar(90)
    ISSELECTED         INT      NOT NULL,   -- int
    TRANSFERSTATUS     INT      NOT NULL,   -- int
    BATCHDESCRIPTION   STRING,              -- nvarchar(max) - nullable
    BATCHEXPIRATIONDATE TIMESTAMP NOT NULL, -- datetime
    BATCHNUMBER        STRING   NOT NULL,   -- nvarchar(20)
    ITEMNUMBER         STRING   NOT NULL,   -- nvarchar(20)
    MANUFACTURINGDATE  TIMESTAMP NOT NULL, -- datetime
    COMPANY            STRING   NOT NULL,   -- nvarchar(4)
    INVENTBATCHRECID   BIGINT   NOT NULL,   -- bigint
    `PARTITION`          STRING   NOT NULL,   -- nvarchar(20)
    DATAAREAID         STRING   NOT NULL,   -- nvarchar(4)
    SYNCSTARTDATETIME  TIMESTAMP NOT NULL, -- datetime
    -- PRIMARY KEY is NOT IMPLEMENTED in Delta Lake
    -- Intended key columns:
    --   EXECUTIONID, BATCHNUMBER, ITEMNUMBER, DATAAREAID, PARTITION
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------
# 3. Verify the table schema (optional)
# ------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventItemBatchStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1073)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIInventItemBatchStaging` (     DEFINITIONGROUP    STRING   NOT NULL,   -- nvarchar(60)     EXECUTIONID        STRING   NOT NULL,   -- nvarchar(90)     ISSELECTED         INT      NOT NULL,   -- int     TRANSFERSTATUS     INT      NOT NULL,   -- int     BATCHDESCRIPTION   STRING,              -- nvarchar(max) - nullable     BATCHEXPIRATIONDATE TIMESTAMP NOT NULL, -- datetime     BATCHNUMBER        STRING   NOT NULL,   -- nvarchar(20)     ITEMNUMBER         STRING   NOT NULL,   -- nvarchar(20)     MANUFACTURINGDATE  TIMESTAMP NOT NULL, -- datetime     COMPANY            STRING   NOT NULL,   -- nvarchar(4)     INVENTBATCHRECID   BIGINT   NOT NULL,   -- bigint     `PARTITION`          STRING   NOT NULL,   -- nvarchar(20)     DATAAREAID         STRING   NOT NULL,   -- nvarchar(4)     SYNCSTARTDATETIME  TIMESTAMP NOT NULL, -- datetime     -- PRIMARY KEY is NOT IMPLEMENTED in Delta Lake     -- Intended key columns:     --   EXECUTIONID, BATCHNUMBER, ITEMNUMBER, DATAAREAID, PARTITION ) USING DELTA
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
