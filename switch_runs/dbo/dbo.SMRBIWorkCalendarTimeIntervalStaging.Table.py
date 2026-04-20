# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWorkCalendarTimeIntervalStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWorkCalendarTimeIntervalStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# 1️⃣  Create the staging table in Delta Lake
# --------------------------------------------------------------------
#   • All references are fully‑qualified: dbe_dbx_internships.dbo.SMRBIWorkCalendarTimeIntervalStaging
#   • T‑SQL types are mapped to Spark SQL types as follows:
#        NVARCHAR → STRING
#        INT       → INT
#        BIGINT    → BIGINT   (Spark already calls it BIGINT)
#        NUMERIC   → DECIMAL(p,s)
#        DATETIME  → TIMESTAMP
#   • PRIMARY KEY constraints are not enforced in Delta Lake; we add a comment
#     explaining that the PK is omitted.
# --------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.dbo.SMRBIWorkCalendarTimeIntervalStaging (
    DEFINITIONGROUP      STRING   NOT NULL,
    EXECUTIONID          STRING   NOT NULL,
    ISSELECTED           INT      NOT NULL,
    TRANSFERSTATUS        INT      NOT NULL,
    WORKCALENDARID       STRING   NOT NULL,
    WORKCALENDARDATE      TIMESTAMP NOT NULL,
    STARTTIME            INT      NOT NULL,
    ENDTIME              INT      NOT NULL,
    EFFICIENCYPERCENTAGE DECIMAL(32,6) NOT NULL,
    COMPANY              STRING   NOT NULL,
    PARTITION            STRING   NOT NULL,
    DATAAREAID           STRING   NOT NULL,
    SYNCSTARTDATETIME    TIMESTAMP NOT NULL,
    RECID                BIGINT   NOT NULL
) USING DELTA
""")

# COMMAND ----------

# Note: PostgreSQL‑style primary keys or index hints are not supported in
# Delta Lake; the original PK definition (CLUSTERED ON… ) is omitted.

# --------------------------------------------------------------------
# 2️⃣  (Optional) Verify creation – e.g. show the table schema
# --------------------------------------------------------------------
spark.sql(f"DESCRIBE dbe_dbx_internships.dbo.SMRBIWorkCalendarTimeIntervalStaging").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
