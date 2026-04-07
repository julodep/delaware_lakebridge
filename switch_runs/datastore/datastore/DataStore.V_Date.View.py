# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Date.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Date.View.sql`

# COMMAND ----------

# Create the view V_Date in the DataStore schema of the target catalog.
# The original T‑SQL view computed a fiscal‑year start date using CONVERT(DATETIME, ... , 103).
# In Spark SQL we use to_timestamp with the corresponding format string ('dd/MM/yyyy').
# All object references are fully‑qualified with `dbe_dbx_internships` and `datastore` placeholders.

sql_query = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`DataStore`.`V_Date` AS
SELECT
    DateCalculation,
    DateTime,
    DayNameLong,
    DayNameShort,
    DayOfWeekId,
    DayOfWeekName,
    DayOfYearId,
    DayOfYearName,
    DimDateId,
    MonthId,
    MonthName,
    MonthOfYearId,
    MonthOfYearName,
    QuarterId,
    QuarterName,
    QuarterOfYearId,
    QuarterOfYearName,
    SemesterId,
    SemesterName,
    SemesterOfYearId,
    SemesterOfYearName,
    WeekId,
    WeekName,
    WeekOfYearId,
    WeekOfYearName,
    YearId,
    YearName,
    CASE
        WHEN MonthOfYearId > 3 THEN to_timestamp(concat('01/04/', cast(YearId as string)), 'dd/MM/yyyy')
        ELSE to_timestamp(concat('01/04/', cast(YearId - 1 as string)), 'dd/MM/yyyy')
    END AS FYStart
FROM `dbe_dbx_internships`.`ETL`.`Date`;
"""

# COMMAND ----------

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
