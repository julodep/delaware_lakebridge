# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Date.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Date.Table.sql`

# COMMAND ----------

# Set the catalog to a catalog where the user has read metadata permissions.
# Adjust the catalog name if a different one is appropriate for your workspace.
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# Create the database if it does not exist.
spark.sql("""
CREATE DATABASE IF NOT EXISTS `DataStore`
""")

# COMMAND ----------

# Create the Date dimension table as a Delta table.
# The FYStart column is defined without the explicit NULL keyword because
# columns are nullable by default in Delta Lake unless NOT NULL is specified.
spark.sql("""
CREATE OR REPLACE TABLE `DataStore`.`Date` (
    DateCalculation STRING NOT NULL,
    DateTime TIMESTAMP NOT NULL,
    DayNameLong STRING NOT NULL,
    DayNameShort STRING NOT NULL,
    DayOfWeekId INT NOT NULL,
    DayOfWeekName STRING NOT NULL,
    DayOfYearId INT NOT NULL,
    DayOfYearName STRING NOT NULL,
    DimDateId INT NOT NULL,
    MonthId INT NOT NULL,
    MonthName STRING NOT NULL,
    MonthOfYearId INT NOT NULL,
    MonthOfYearName STRING NOT NULL,
    QuarterId INT NOT NULL,
    QuarterName STRING NOT NULL,
    QuarterOfYearId INT NOT NULL,
    QuarterOfYearName STRING NOT NULL,
    SemesterId INT NOT NULL,
    SemesterName STRING NOT NULL,
    SemesterOfYearId INT NOT NULL,
    SemesterOfYearName STRING NOT NULL,
    WeekId INT NOT NULL,
    WeekName STRING NOT NULL,
    WeekOfYearId INT NOT NULL,
    WeekOfYearName STRING NOT NULL,
    YearId INT NOT NULL,
    YearName STRING NOT NULL,
    FYStart TIMESTAMP  -- nullable by default
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 982)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `DataStore`.`Date` (     DateCalculation STRING NOT NULL,     DateTime TIMESTAMP NOT NULL,     DayNameLong STRING NOT NULL,     DayNameShort STRING NOT NULL,     DayOfWeekId INT NOT NULL,     DayOfWeekName STRING NOT NULL,     DayOfYearId INT NOT NULL,     DayOfYearName STRING NOT NULL,     DimDateId INT NOT NULL,     MonthId INT NOT NULL,     MonthName STRING NOT NULL,     MonthOfYearId INT NOT NULL,     MonthOfYearName STRING NOT NULL,     QuarterId INT NOT NULL,     QuarterName STRING NOT NULL,     QuarterOfYearId INT NOT NULL,     QuarterOfYearName STRING NOT NULL,     SemesterId INT NOT NULL,     SemesterName STRING NOT NULL,     SemesterOfYearId INT NOT NULL,     SemesterOfYearName STRING NOT NULL,     WeekId INT NOT NULL,     WeekName STRING NOT NULL,     WeekOfYearId INT NOT NULL,     WeekOfYearName STRING NOT NULL,     YearId INT NOT NULL,     YearName STRING NOT NULL,     FYStart TIMESTAMP  -- nullable by default ) USING DELTA
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC ```
