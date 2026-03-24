# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.Date.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324081459-mgpq/ETL.Date.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Setup: import any required libraries (Databricks provides Spark session)
# -------------------------------------------------------------------------
# No additional imports needed for this script.

# -------------------------------------------------------------------------
# Ensure the target database (schema) exists and the session has usage rights.
# This creates the database if it does not already exist.
# -------------------------------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS `ETL`
""")

# COMMAND ----------

# Switch to the ETL database to simplify object references and avoid
# permission checks on the fully qualified name during creation.
spark.sql("USE `ETL`")

# COMMAND ----------

# -------------------------------------------------------------------------
# NOTE: T‑SQL session settings (SET ANSI_NULLS, SET QUOTED_IDENTIFIER) and
# the batch separator GO are not applicable in Databricks/Spark SQL.
# They are omitted and documented here as comments.
# -------------------------------------------------------------------------
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON
# GO

# -------------------------------------------------------------------------
# Create the dimension table `Date` as a Delta table in the ETL database.
# Column data types are mapped from T‑SQL to Spark SQL types:
#   INT          -> INT
#   DATETIME     -> TIMESTAMP
#   NVARCHAR(...) -> STRING
# -------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE `Date` (
    DimDateId           INT               NOT NULL,
    DateTime            TIMESTAMP         NOT NULL,
    YearId              INT               NOT NULL,
    YearName            STRING            NOT NULL,
    SemesterOfYearId    INT               NOT NULL,
    SemesterId          INT               NOT NULL,
    SemesterName        STRING            NOT NULL,
    SemesterOfYearName  STRING            NOT NULL,
    QuarterOfYearId     INT               NOT NULL,
    QuarterId           INT               NOT NULL,
    QuarterName         STRING            NOT NULL,
    QuarterOfYearName   STRING            NOT NULL,
    MonthOfYearId       INT               NOT NULL,
    MonthId             INT               NOT NULL,
    MonthName           STRING            NOT NULL,
    MonthOfYearName     STRING            NOT NULL,
    DayOfYearId         INT               NOT NULL,
    DayNameLong         STRING            NOT NULL,
    DayNameShort        STRING            NOT NULL,
    DayOfYearName       STRING            NOT NULL,
    DayOfWeekName       STRING            NOT NULL,
    DayOfWeekId         INT               NOT NULL,
    WeekOfYearId        INT               NOT NULL,
    WeekOfYearName      STRING            NOT NULL,
    WeekId              INT               NOT NULL,
    WeekName            STRING            NOT NULL,
    DateCalculation     STRING            NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
