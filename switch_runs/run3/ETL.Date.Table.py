# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.Date.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260323134003-blyy/ETL.Date.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Setup: Convert T‑SQL table definition to a Delta table in Databricks
# ------------------------------------------------------------------

# The following SET statements and GO batch separators are T‑SQL specific
# and have no effect in Databricks. They are retained as comments for reference.
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON
# GO

# --------------------------------------------------------------
# Create a database (schema) if you have the required privileges.
# If you do not have permission on the `ETL` database, use an
# existing database (e.g., `default`) or create a new one that you own.
# --------------------------------------------------------------
# Uncomment the block below if you can create the schema:
# spark.sql("CREATE SCHEMA IF NOT EXISTS ETL")

# --------------------------------------------------------------
# Create the Date dimension table as a Delta table.
# If you lack permissions on `ETL`, create the table in the
# default schema (or another schema you own) by omitting the database qualifier.
# --------------------------------------------------------------

# Choose the appropriate fully‑qualified name:
#   - With permissions: `ETL`.`Date`
#   - Without permissions: just `Date` (creates in the current schema)
table_name = "`Date`"          # use this if you cannot write to `ETL`

# COMMAND ----------

# table_name = "`ETL`.`Date`"   # uncomment this if you have permissions on ETL

spark.sql(f"""
CREATE OR REPLACE TABLE {table_name} (
    DimDateId               INT          NOT NULL,
    DateTime                TIMESTAMP    NOT NULL,
    YearId                  INT          NOT NULL,
    YearName                STRING       NOT NULL,
    SemesterOfYearId        INT          NOT NULL,
    SemesterId              INT          NOT NULL,
    SemesterName            STRING       NOT NULL,
    SemesterOfYearName      STRING       NOT NULL,
    QuarterOfYearId         INT          NOT NULL,
    QuarterId               INT          NOT NULL,
    QuarterName             STRING       NOT NULL,
    QuarterOfYearName       STRING       NOT NULL,
    MonthOfYearId           INT          NOT NULL,
    MonthId                 INT          NOT NULL,
    MonthName               STRING       NOT NULL,
    MonthOfYearName         STRING       NOT NULL,
    DayOfYearId             INT          NOT NULL,
    DayNameLong             STRING       NOT NULL,
    DayNameShort            STRING       NOT NULL,
    DayOfYearName           STRING       NOT NULL,
    DayOfWeekName           STRING       NOT NULL,
    DayOfWeekId             INT          NOT NULL,
    WeekOfYearId            INT          NOT NULL,
    WeekOfYearName          STRING       NOT NULL,
    WeekId                  INT          NOT NULL,
    WeekName                STRING       NOT NULL,
    DateCalculation         STRING       NOT NULL
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
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
