# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Date.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Date.Table.sql`

# COMMAND ----------

# Optional: drop the table if it already exists (uncomment if needed)
# spark.sql("DROP TABLE IF EXISTS `dbe_dbx_internships`.`datastore`.`Date`")

# Create or replace the Unity Catalog table with correct Spark SQL syntax
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Date` (
    DateCalculation   STRING  NOT NULL,
    DateTime          TIMESTAMP NOT NULL,
    DayNameLong       STRING  NOT NULL,
    DayNameShort      STRING  NOT NULL,
    DayOfWeekId       INT     NOT NULL,
    DayOfWeekName     STRING  NOT NULL,
    DayOfYearId       INT     NOT NULL,
    DayOfYearName     STRING  NOT NULL,
    DimDateId         INT     NOT NULL,
    MonthId           INT     NOT NULL,
    MonthName         STRING  NOT NULL,
    MonthOfYearId     INT     NOT NULL,
    MonthOfYearName   STRING  NOT NULL,
    QuarterId         INT     NOT NULL,
    QuarterName       STRING  NOT NULL,
    QuarterOfYearId   INT     NOT NULL,
    QuarterOfYearName STRING  NOT NULL,
    SemesterId        INT     NOT NULL,
    SemesterName      STRING  NOT NULL,
    SemesterOfYearId  INT     NOT NULL,
    SemesterOfYearName STRING NOT NULL,
    WeekId            INT     NOT NULL,
    WeekName          STRING  NOT NULL,
    WeekOfYearId      INT     NOT NULL,
    WeekOfYearName    STRING  NOT NULL,
    YearId            INT     NOT NULL,
    YearName          STRING  NOT NULL,
    FYStart           TIMESTAMP
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
