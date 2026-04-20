# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWorkCalendarDayStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWorkCalendarDayStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------
# 1. Create the table in the target catalog & schema
# --------------------------------------------------
# Spark SQL uses backticks for fully‑qualified identifiers.
# Primary key constraints are not enforced in Delta Lake, so they are omitted
# and a comment explains the omission.

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWorkCalendarDayStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    WORKCALENDARID STRING NOT NULL,
    CALENDARDATE  TIMESTAMP NOT NULL,
    COMPANY        STRING NOT NULL,
    PARTITION      STRING NOT NULL,
    DATAAREAID     STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID          LONG NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# --------------------------------------------------
# 2. (Optional) Verify the schema
# --------------------------------------------------
# This statement prints the table’s schema to the notebook output.
display(spark.table(f"dbe_dbx_internships.dbo.SMRBIWorkCalendarDayStaging"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
