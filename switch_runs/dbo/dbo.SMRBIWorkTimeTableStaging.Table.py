# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWorkTimeTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWorkTimeTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table in Databricks using Delta Lake syntax.
# ------------------------------------------------------------------
# NOTE:  In Spark SQL we cannot specify a primary key or clustered index
#        the way SQL Server does.  If unique enforcement is needed,
#        consider adding a unique-constraint-enforcing query or a
#        separate validation job.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWorkTimeTableStaging` (
    -- NVARCHAR(60) -> STRING, INT -> INT, DATETIME -> TIMESTAMP
    DEFINITIONGROUP        STRING,
    EXECUTIONID            STRING,
    ISSELECTED             INT,
    TRANSFERSTATUS         INT,
    CLOSEDFORPICKUPFRIDAY  INT,
    CLOSEDFORPICKUPMONDAY  INT,
    CLOSEDFORPICKUPSATURDAY INT,
    CLOSEDFORPICKUPSUNDAY  INT,
    CLOSEDFORPICKUPTHURSDAY INT,
    CLOSEDFORPICKUPTUESDAY INT,
    CLOSEDFORPICKUPWEDNESDAY INT,
    NAME                   STRING,
    WORKTIMEID             STRING,
    PARTITION              STRING,
    DATAAREAID             STRING,
    SYNCSTARTDATETIME      TIMESTAMP
);
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Verify the new table's schema has been created
# ------------------------------------------------------------------
df = spark.table(f"`dbe_dbx_internships`.`dbo`.`SMRBIWorkTimeTableStaging`")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 700)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIWorkTimeTableStaging` (     -- NVARCHAR(60) -> STRING, INT -> INT, DATETIME -> TIMESTAMP     DEFINITIONGROUP        STRING,     EXECUTIONID            STRING,     ISSELECTED             INT,     TRANSFERSTATUS         INT,     CLOSEDFORPICKUPFRIDAY  INT,     CLOSEDFORPICKUPMONDAY  INT,     CLOSEDFORPICKUPSATURDAY INT,     CLOSEDFORPICKUPSUNDAY  INT,     CLOSEDFORPICKUPTHURSDAY INT,     CLOSEDFORPICKUPTUESDAY INT,     CLOSEDFORPICKUPWEDNESDAY INT,     NAME                   STRING,     WORKTIMEID             STRING,     PARTITION              STRING,     DATAAREAID             STRING,     SYNCSTARTDATETIME      TIMESTAMP );
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
