# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.Date.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/etl/etl_volume/etl/ETL.Date.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1. Purpose
# ------------------------------------------------------------------
# This script creates the equivalent of the T‑SQL statement
#   CREATE TABLE [ETL].[Date] ( … )
# for use in Databricks (Unity Catalog).  
#
# • int          ➜  INT
# • datetime     ➜  TIMESTAMP
# • nvarchar(n)  ➜  STRING
# • All columns are declared NOT NULL exactly as in the source.
#
# The table will be created as a Delta table, which is the default
# storage format in Databricks.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`Date` (
    DimDateId            INT          NOT NULL,
    DateTime             TIMESTAMP    NOT NULL,
    YearId               INT          NOT NULL,
    YearName             STRING       NOT NULL,
    SemesterOfYearId     INT          NOT NULL,
    SemesterId           INT          NOT NULL,
    SemesterName         STRING       NOT NULL,
    SemesterOfYearName   STRING       NOT NULL,
    QuarterOfYearId     INT          NOT NULL,
    QuarterId            INT          NOT NULL,
    QuarterName          STRING       NOT NULL,
    QuarterOfYearName    STRING       NOT NULL,
    MonthOfYearId        INT          NOT NULL,
    MonthId              INT          NOT NULL,
    MonthName            STRING       NOT NULL,
    MonthOfYearName      STRING       NOT NULL,
    DayOfYearId          INT          NOT NULL,
    DayNameLong          STRING       NOT NULL,
    DayNameShort         STRING       NOT NULL,
    DayOfYearName        STRING       NOT NULL,
    DayOfWeekName        STRING       NOT NULL,
    DayOfWeekId          INT          NOT NULL,
    WeekOfYearId         INT          NOT NULL,
    WeekOfYearName       STRING       NOT NULL,
    WeekId               INT          NOT NULL,
    WeekName              STRING       NOT NULL,
    DateCalculation      STRING       NOT NULL
);
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2. Post‑creation check
# ------------------------------------------------------------------
# Verify that the table exists and the schema is as expected
spark.sql(f"DESC TABLE `{catalog}`.`{schema}`.`Date`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
