# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.FiscalPeriodStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.FiscalPeriodStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the staging table FiscalPeriodStaging
# ------------------------------------------------------------------
# Note:
#   • All object references use the fully‑qualified format:
#     `dbe_dbx_internships`.`dbo`.`FiscalPeriodStaging`
#   • T‑SQL data types are mapped to Spark SQL types:
#       NVARCHAR → STRING
#       INT      → INT
#       BIGINT   → LONG
#       DATETIME → TIMESTAMP
#   • The PRIMARY KEY definition cannot be expressed in Delta Lake.
#     It is therefore omitted and the original constraint is
#     mentioned in the comment below.
#
# ------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE `dbe_dbx_internships`.`dbo`.`FiscalPeriodStaging` (
        DEFINITIONGROUP STRING NOT NULL,
        EXECUTIONID    STRING NOT NULL,
        ISSELECTED     INT NOT NULL,
        TRANSFERSTATUS INT NOT NULL,
        COMMENTS       STRING NOT NULL,
        ENDDATE        TIMESTAMP NOT NULL,
        MONTH          INT NOT NULL,
        PERIODNAME     STRING NOT NULL,
        QUARTER        INT NOT NULL,
        SHORTNAME      STRING NOT NULL,
        STARTDATE      TIMESTAMP NOT NULL,
        TYPE           INT NOT NULL,
        CALENDAR       STRING NOT NULL,
        FISCALYEAR     STRING NOT NULL,
        CALENDARTYPE   INT NOT NULL,
        DAYS           INT NOT NULL,
        PARTITION      STRING NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL,
        RECID          LONG NOT NULL
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
