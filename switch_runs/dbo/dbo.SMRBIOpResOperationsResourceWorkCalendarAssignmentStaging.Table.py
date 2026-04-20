# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIOpResOperationsResourceWorkCalendarAssignmentStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIOpResOperationsResourceWorkCalendarAssignmentStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# NOTE: All object references use the fully‑qualified format:
#       `dbe_dbx_internships`.`dbo`.<table_name>
# ------------------------------------------------------------------

# Create the staging table in Delta format.  Data types that are
# equivalent between T‑SQL and Spark SQL are mapped as follows:
#   NVARCHAR → STRING
#   INT      → INT
#   BIGINT   → LONG
#   DATETIME → TIMESTAMP
#
# The original T‑SQL definition contains a PRIMARY KEY clause.  Delta
# Lake does not enforce primary keys at write time, so the clause
# is omitted in the production definition.  If you need a logical
# key for downstream processing, include it in the schema as a comment
# or use the `delta.constraints` property in a future release.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIOpResOperationsResourceWorkCalendarAssignmentStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    OPERATIONSRESOURCEID STRING NOT NULL,
    WORKCALENDARID STRING NOT NULL,
    VALIDFROM TIMESTAMP NOT NULL,
    VALIDTO TIMESTAMP NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
