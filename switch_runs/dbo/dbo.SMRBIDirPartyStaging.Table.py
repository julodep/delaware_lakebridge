# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDirPartyStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDirPartyStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table `SMRBIDirPartyStaging` in the specified catalog and schema.
#
# * All object names are fully‑qualified: `dbe_dbx_internships.dbo.SMRBIDirPartyStaging`
# * Data types have been mapped from T‑SQL to Spark SQL equivalents.
# * A primary‑key constraint is present in T‑SQL, but Delta Lake does not enforce
#   primary keys.  The constraint is therefore commented out to preserve the
#   original definition for reference.
# ------------------------------------------------------------------

spark.sql(f"""
-- Drop the table if it already exists to avoid duplicate‑definition errors
DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIDirPartyStaging`;

-- Create the table with the appropriate schema
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIDirPartyStaging` (
    DEFINITIONGROUP          STRING,
    EXECUTIONID              STRING,
    ISSELECTED               INT,
    TRANSFERSTATUS           INT,
    PARTYNUMBER              STRING,
    NAMEALIAS                STRING,
    LEGALENTITYDATAAREA      STRING,
    OPERATINGUNITNUMBER      STRING,
    OPERATINGUNITTYPE        INT,
    DIRPARTYRECID            BIGINT,
    VALIDFROM                TIMESTAMP,
    VALIDTO                  TIMESTAMP,
    `PARTITION`               STRING,
    SYNCSTARTDATETIME        TIMESTAMP,
    RECID                    BIGINT
    -- NOTE: T‑SQL defines a primary key on (EXECUTIONID, PARTYNUMBER, PARTITION),
    --       which Delta Lake does not support natively.
    -- PRIMARY KEY (EXECUTIONID, PARTYNUMBER, PARTITION)
)
USING DELTA;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1046)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN -- Drop the table if it already exists to avoid duplicate‑definition errors DROP TABLE IF EXISTS `_placeholder_`.`_placeholder_`.`SMRBIDirPartyStaging`;  -- Create the table with the appropriate schema CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBIDirPartyStaging` (     DEFINITIONGROUP          STRING,     EXECUTIONID              STRING,     ISSELECTED               INT,     TRANSFERSTATUS           INT,     PARTYNUMBER              STRING,     NAMEALIAS                STRING,     LEGALENTITYDATAAREA      STRING,     OPERATINGUNITNUMBER      STRING,     OPERATINGUNITTYPE        INT,     DIRPARTYRECID            BIGINT,     VALIDFROM                TIMESTAMP,     VALIDTO                  TIMESTAMP,     `PARTITION`               STRING,     SYNCSTARTDATETIME        TIMESTAMP,     RECID                    BIGINT     -- NOTE: T‑SQL defines a primary key on (EXECUTIONID, PARTYNUMBER, PARTITION),     --       which Delta Lake does not support natively.     -- PRIMARY KEY (EXECUTIONID, PARTYNUMBER, PARTITION) ) USING DELTA;
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
