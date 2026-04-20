# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWorkCalendarStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWorkCalendarStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Create the staging table `SMRBIWorkCalendarStaging` in the target catalog/schema.
# -------------------------------------------------------------
# In Databricks/Delta Lake the exact primary-key/clustered-index definition from SQL Server
# cannot be recreated as a native feature.  If you need to enforce uniqueness you can 
# add a secondary validation step or use Delta Lake ACID guarantees after writes.
# -------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWorkCalendarStaging` (
    -- 1. DEFINITIONGROUP  NVARCHAR(60)  → STRING
    DEFINITIONGROUP STRING NOT NULL,

    -- 2. EXECUTIONID      NVARCHAR(90)  → STRING
    EXECUTIONID STRING NOT NULL,

    -- 3. ISSELECTED       INT          → INT
    ISSELECTED INT NOT NULL,

    -- 4. TRANSFERSTATUS   INT          → INT
    TRANSFERSTATUS INT NOT NULL,

    -- 5. CALENDARID       NVARCHAR(10)  → STRING
    CALENDARID STRING NOT NULL,

    -- 6. CALENDARNAME     NVARCHAR(60)  → STRING
    CALENDARNAME STRING NOT NULL,

    -- 7. WORKHOURS        NUMERIC(32,6) → DECIMAL(32,6)
    WORKHOURS DECIMAL(32,6) NOT NULL,

    -- 8. COMPANY          NVARCHAR(4)   → STRING
    COMPANY STRING NOT NULL,

    -- 9. BASICCALENDARID NVARCHAR(10)  → STRING
    BASICCALENDARID STRING NOT NULL,

    --10. PARTITION        NVARCHAR(20)  → STRING
    `PARTITION` STRING NOT NULL,

    --11. DATAAREAID       NVARCHAR(4)   → STRING
    DATAAREAID STRING NOT NULL,

    --12. SYNCSTARTDATETIME DATETIME  → TIMESTAMP
    SYNCSTARTDATETIME TIMESTAMP NOT NULL

    -- Note: The original table had a composite PRIMARY KEY and
    -- several index/cluster options. In Delta Lake those are not
    -- expressed in the CREATE TABLE statement.  If you require 
    -- uniqueness guarantees you can add a Delta Lake "unique"/"primary key"
    -- constraint (introduced in later Databricks runtimes) or enforce
    -- it at the application level.
);
""")

# COMMAND ----------

# -------------------------------------------------------------
# Verify the schema of the newly created table
# -------------------------------------------------------------
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIWorkCalendarStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1492)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIWorkCalendarStaging` (     -- 1. DEFINITIONGROUP  NVARCHAR(60)  → STRING     DEFINITIONGROUP STRING NOT NULL,      -- 2. EXECUTIONID      NVARCHAR(90)  → STRING     EXECUTIONID STRING NOT NULL,      -- 3. ISSELECTED       INT          → INT     ISSELECTED INT NOT NULL,      -- 4. TRANSFERSTATUS   INT          → INT     TRANSFERSTATUS INT NOT NULL,      -- 5. CALENDARID       NVARCHAR(10)  → STRING     CALENDARID STRING NOT NULL,      -- 6. CALENDARNAME     NVARCHAR(60)  → STRING     CALENDARNAME STRING NOT NULL,      -- 7. WORKHOURS        NUMERIC(32,6) → DECIMAL(32,6)     WORKHOURS DECIMAL(32,6) NOT NULL,      -- 8. COMPANY          NVARCHAR(4)   → STRING     COMPANY STRING NOT NULL,      -- 9. BASICCALENDARID NVARCHAR(10)  → STRING     BASICCALENDARID STRING NOT NULL,      --10. PARTITION        NVARCHAR(20)  → STRING     `PARTITION` STRING NOT NULL,      --11. DATAAREAID       NVARCHAR(4)   → STRING     DATAAREAID STRING NOT NULL,      --12. SYNCSTARTDATETIME DATETIME  → TIMESTAMP     SYNCSTARTDATETIME TIMESTAMP NOT NULL      -- Note: The original table had a composite PRIMARY KEY and     -- several index/cluster options. In Delta Lake those are not     -- expressed in the CREATE TABLE statement.  If you require      -- uniqueness guarantees you can add a Delta Lake "unique"/"primary key"     -- constraint (introduced in later Databricks runtimes) or enforce     -- it at the application level. );
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
