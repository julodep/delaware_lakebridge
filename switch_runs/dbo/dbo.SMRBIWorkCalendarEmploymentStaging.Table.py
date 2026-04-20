# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWorkCalendarEmploymentStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWorkCalendarEmploymentStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Define your Unity Catalog catalog and schema names here
# ------------------------------------------------------------------
catalog = "your_catalog_name"   # e.g., "finance_catalog"
schema  = "your_schema_name"    # e.g., "work_calendar"

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table that mirrors the original T‑SQL schema.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWorkCalendarEmploymentStaging` (
    -- NVARCHAR(60) → STRING
    DEFINITIONGROUP STRING,
    -- NVARCHAR(90) → STRING
    EXECUTIONID   STRING,
    -- INT → INT
    ISSELECTED    INT,
    -- INT → INT
    TRANSFERSTATUS INT,
    -- NVARCHAR(4) → STRING
    CALENDARDATAAREAID STRING,
    -- NVARCHAR(10) → STRING
    CALENDARID   STRING,
    -- DATETIME → TIMESTAMP
    HCMEMPLOYMENT_FK_VALIDFROM TIMESTAMP,
    -- DATETIME → TIMESTAMP
    HCMEMPLOYMENT_FK_VALIDTO TIMESTAMP,
    -- NVARCHAR(40) → STRING
    COMPANYINFO_FK_PARTYNUMBER STRING,
    -- NVARCHAR(4) → STRING
    COMPANYINFO_FK_DATAAREA STRING,
    -- NVARCHAR(25) → STRING
    HCMWORKER_FK_PERSONNELNUMBER STRING,
    -- NVARCHAR(40) → STRING
    DIRPERSON_FK_PARTYNUMBER STRING,
    -- NVARCHAR(20) → STRING
    PARTITION    STRING,
    -- DATETIME → TIMESTAMP
    SYNCSTARTDATETIME TIMESTAMP,
    -- BIGINT → LONG
    RECID        LONG
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 993)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `your_catalog_name`.`your_schema_name`.`SMRBIWorkCalendarEmploymentStaging` (     -- NVARCHAR(60) → STRING     DEFINITIONGROUP STRING,     -- NVARCHAR(90) → STRING     EXECUTIONID   STRING,     -- INT → INT     ISSELECTED    INT,     -- INT → INT     TRANSFERSTATUS INT,     -- NVARCHAR(4) → STRING     CALENDARDATAAREAID STRING,     -- NVARCHAR(10) → STRING     CALENDARID   STRING,     -- DATETIME → TIMESTAMP     HCMEMPLOYMENT_FK_VALIDFROM TIMESTAMP,     -- DATETIME → TIMESTAMP     HCMEMPLOYMENT_FK_VALIDTO TIMESTAMP,     -- NVARCHAR(40) → STRING     COMPANYINFO_FK_PARTYNUMBER STRING,     -- NVARCHAR(4) → STRING     COMPANYINFO_FK_DATAAREA STRING,     -- NVARCHAR(25) → STRING     HCMWORKER_FK_PERSONNELNUMBER STRING,     -- NVARCHAR(40) → STRING     DIRPERSON_FK_PARTYNUMBER STRING,     -- NVARCHAR(20) → STRING     PARTITION    STRING,     -- DATETIME → TIMESTAMP     SYNCSTARTDATETIME TIMESTAMP,     -- BIGINT → LONG     RECID        LONG ) USING DELTA
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
