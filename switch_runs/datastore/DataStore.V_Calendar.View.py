# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Calendar.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastoredemo/DataStore.V_Calendar.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  View creation:  V_Calendar
#
#  The original T‑SQL used SET commands and a GO batch separator.
#  Those are ignored by Spark/Databricks.  The view is created as a
#  persistent view in the target catalog (catalog) and schema (schema).
#
#  Fully‑qualified object names are used to avoid ambiguity:
#      `dbe_dbx_internships`.`datastore`.`V_Calendar`
# ------------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE VIEW DataStore.`V_Calendar` AS
SELECT
    WCS.CalendarId   AS CalendarCode,
    WCS.CalendarName AS CalendarName,
    WCS.DataAreaId   AS CompanyCode,
    WCS.WorkHours    AS StandardWorkDayHours
FROM   dbo.`SMRBIWorkCalendarStaging` WCS
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
