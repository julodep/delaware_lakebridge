# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventBuyerGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventBuyerGroupStaging.Table.sql`

# COMMAND ----------

# -----------------------------------------------------------
# Create the staging table in the target catalog and schema
# -----------------------------------------------------------
# T‑SQL columns and data types are mapped to Spark SQL types as follows:
#   nvarchar  → STRING
#   int       → INT
#   datetime  → TIMESTAMP
#
# Delta Lake (Databricks) does not support inline PRIMARY KEY or CLUSTERED
# constraints.  We will create the table schema directly and add a
# comment noting the intended uniqueness.  If you require
# uniqueness enforcement, create a unique index after data load.
#
# Fully‑qualified table name:  dbe_dbx_internships.dbo.SMRBIInventBuyerGroupStaging
# -----------------------------------------------------------

# Drop the table if it already exists
spark.sql(f"""
DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIInventBuyerGroupStaging`
""")

# COMMAND ----------

# Create the table with the specified columns
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventBuyerGroupStaging` (
    DEFINITIONGROUP  STRING,                -- nvarchar(60)
    EXECUTIONID      STRING,                -- nvarchar(90)
    ISSELECTED       INT,                   -- int
    TRANSFERSTATUS   INT,                   -- int
    GROUPDESCRIPTION STRING,                -- nvarchar(60)
    GROUPID          STRING,                -- nvarchar(10)
    COMPANY          STRING,                -- nvarchar(4)
    PARTITION        STRING,                -- nvarchar(20)
    DATAAREAID       STRING,                -- nvarchar(4)
    SYNCSTARTDATETIME TIMESTAMP              -- datetime
    -- Primary key (EXECUTIONID, GROUPID, DATAAREAID, PARTITION)
    -- is not enforced by Delta Lake.  Add a unique index after data load if needed.
)
USING DELTA
""")

# COMMAND ----------

# -----------------------------------------------------------
# Verify the table creation by showing its schema
# -----------------------------------------------------------
spark.sql(
    f"DESCRIBE STRUCTURE `dbe_dbx_internships`.`dbo`.`SMRBIInventBuyerGroupStaging`"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 826)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBIInventBuyerGroupStaging` (     DEFINITIONGROUP  STRING,                -- nvarchar(60)     EXECUTIONID      STRING,                -- nvarchar(90)     ISSELECTED       INT,                   -- int     TRANSFERSTATUS   INT,                   -- int     GROUPDESCRIPTION STRING,                -- nvarchar(60)     GROUPID          STRING,                -- nvarchar(10)     COMPANY          STRING,                -- nvarchar(4)     PARTITION        STRING,                -- nvarchar(20)     DATAAREAID       STRING,                -- nvarchar(4)     SYNCSTARTDATETIME TIMESTAMP              -- datetime     -- Primary key (EXECUTIONID, GROUPID, DATAAREAID, PARTITION)     -- is not enforced by Delta Lake.  Add a unique index after data load if needed. ) USING DELTA
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
