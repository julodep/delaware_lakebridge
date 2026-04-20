# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjForecastEmplStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjForecastEmplStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the SMRBIProjForecastEmplStaging table in Unity Catalog
#  (catalog and schema names are placeholders – replace them with your
#   actual catalog and schema values before running).
# ------------------------------------------------------------------

# NOTE:  Primary‑key constraints are not enforced in Delta Lake/Unity
# Catalog.  If you need a unique index, you can add it manually after
# the table is created (e.g. `spark.sql("ALTER TABLE ... ADD UNIQUE (...)")`).
# ------------------------------------------------------------------

# Make sure to substitute the placeholders with your actual catalog &
# schema names before executing this block.
catalog = '<your_catalog>'
schema  = '<your_schema>'

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastEmplStaging` (
    CATEGORY         STRING   NOT NULL,      -- nvarchar(30)
    LINEPROPERTY     STRING   NOT NULL,      -- nvarchar(10)
    PROJECTID        STRING   NOT NULL,      -- nvarchar(20)
    SALESCURRENCY    STRING   NOT NULL,      -- nvarchar(3)
    TRANSACTIONID    STRING   NOT NULL,      -- nvarchar(20)
    DEFINITIONGROUP  STRING   NOT NULL,      -- nvarchar(60)
    EXECUTIONID      STRING   NOT NULL,      -- nvarchar(90)
    ISSELECTED       INT      NOT NULL,      -- int
    TRANSFERSTATUS   INT      NOT NULL,      -- int
    ACTIVITYNUMBER   STRING   NOT NULL,      -- nvarchar(50)
    COSTPRICE        DECIMAL(32,6) NOT NULL, -- numeric(32,6)
    COMPANY          STRING   NOT NULL,      -- nvarchar(4)
    DESCRIPTION      STRING   NOT NULL,      -- nvarchar(60)
    FORECASTMODEL    STRING   NOT NULL,      -- nvarchar(10)
    HOURS            DECIMAL(32,6) NOT NULL, -- numeric(32,6)
    PROJECTDATE      TIMESTAMP NOT NULL,    -- datetime
    SALESPRICE       DECIMAL(32,6) NOT NULL, -- numeric(32,6)
    WORKER           BIGINT   NOT NULL,      -- bigint
    PARTITION_COLUMN STRING   NOT NULL,      -- nvarchar(20)
    DATAAREAID       STRING   NOT NULL,      -- nvarchar(4)
    SYNCSTARTDATETIME TIMESTAMP NOT NULL    -- datetime
    -- Primary key definition omitted: Delta Lake does not enforce
    -- primary‑key constraints automatically. Add a UNIQUE constraint
    -- via ALTER TABLE if required.
)
""")

# COMMAND ----------

# ------------------------------------------------------------------
#  Verify that the table was created successfully:
# ------------------------------------------------------------------
spark.sql(f"DESCRIBE DETAIL `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastEmplStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1519)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `<your_catalog>`.`<your_schema>`.`SMRBIProjForecastEmplStaging` (     CATEGORY         STRING   NOT NULL,      -- nvarchar(30)     LINEPROPERTY     STRING   NOT NULL,      -- nvarchar(10)     PROJECTID        STRING   NOT NULL,      -- nvarchar(20)     SALESCURRENCY    STRING   NOT NULL,      -- nvarchar(3)     TRANSACTIONID    STRING   NOT NULL,      -- nvarchar(20)     DEFINITIONGROUP  STRING   NOT NULL,      -- nvarchar(60)     EXECUTIONID      STRING   NOT NULL,      -- nvarchar(90)     ISSELECTED       INT      NOT NULL,      -- int     TRANSFERSTATUS   INT      NOT NULL,      -- int     ACTIVITYNUMBER   STRING   NOT NULL,      -- nvarchar(50)     COSTPRICE        DECIMAL(32,6) NOT NULL, -- numeric(32,6)     COMPANY          STRING   NOT NULL,      -- nvarchar(4)     DESCRIPTION      STRING   NOT NULL,      -- nvarchar(60)     FORECASTMODEL    STRING   NOT NULL,      -- nvarchar(10)     HOURS            DECIMAL(32,6) NOT NULL, -- numeric(32,6)     PROJECTDATE      TIMESTAMP NOT NULL,    -- datetime     SALESPRICE       DECIMAL(32,6) NOT NULL, -- numeric(32,6)     WORKER           BIGINT   NOT NULL,      -- bigint     PARTITION_COLUMN STRING   NOT NULL,      -- nvarchar(20)     DATAAREAID       STRING   NOT NULL,      -- nvarchar(4)     SYNCSTARTDATETIME TIMESTAMP NOT NULL    -- datetime     -- Primary key definition omitted: Delta Lake does not enforce     -- primary‑key constraints automatically. Add a UNIQUE constraint     -- via ALTER TABLE if required. )
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
