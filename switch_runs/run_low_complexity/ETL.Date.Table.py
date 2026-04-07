# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.Date.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324104824-3bci/ETL.Date.Table.sql`

# COMMAND ----------

# Setup: import any required libraries (Databricks provides Spark session by default)
import sys

# COMMAND ----------

# ------------------------------------------------------------
# The following T‑SQL options (ANSI_NULLS, QUOTED_IDENTIFIER) are
# not applicable in Databricks / Spark SQL and are therefore omitted.
# ------------------------------------------------------------

# Create the Date dimension table in the target catalog and schema.
# Original schema [ETL] is mapped to the required schema `switchschema`.
spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.Date (
    DimDateId          INT           NOT NULL,
    DateTime           TIMESTAMP     NOT NULL,
    YearId             INT           NOT NULL,
    YearName           STRING        NOT NULL,
    SemesterOfYearId   INT           NOT NULL,
    SemesterId         INT           NOT NULL,
    SemesterName       STRING        NOT NULL,
    SemesterOfYearName STRING        NOT NULL,
    QuarterOfYearId    INT           NOT NULL,
    QuarterId          INT           NOT NULL,
    QuarterName        STRING        NOT NULL,
    QuarterOfYearName  STRING        NOT NULL,
    MonthOfYearId      INT           NOT NULL,
    MonthId            INT           NOT NULL,
    MonthName          STRING        NOT NULL,
    MonthOfYearName    STRING        NOT NULL,
    DayOfYearId        INT           NOT NULL,
    DayNameLong        STRING        NOT NULL,
    DayNameShort       STRING        NOT NULL,
    DayOfYearName      STRING        NOT NULL,
    DayOfWeekName      STRING        NOT NULL,
    DayOfWeekId        INT           NOT NULL,
    WeekOfYearId       INT           NOT NULL,
    WeekOfYearName     STRING        NOT NULL,
    WeekId             INT           NOT NULL,
    WeekName           STRING        NOT NULL,
    DateCalculation    STRING        NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
