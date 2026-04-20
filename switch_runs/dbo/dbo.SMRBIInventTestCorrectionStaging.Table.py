# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventTestCorrectionStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventTestCorrectionStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# 1. Define fully‑qualified table name
# -------------------------------------------------------------
catalog = "<catalog>"          # replace with your catalog name
schema  = "<schema>"           # replace with your schema name
table_name = "SMRBIInventTestCorrectionStaging"

# COMMAND ----------

# -------------------------------------------------------------
# 2. Create the table
# -------------------------------------------------------------
# The CREATE TABLE statement mirrors the T-SQL DDL.  
# We use DELTA as the storage format; indices / PKs are not supported in Delta Lake,
# so the PRIMARY KEY clause is omitted.
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`{table_name}` (
    DEFINITIONGROUP            STRING   NOT NULL,
    EXECUTIONID                STRING   NOT NULL,
    ISSELECTED                 INT      NOT NULL,
    TRANSFERSTATUS             INT      NOT NULL,
    CORRECTIONCOMPLETED        INT      NOT NULL,
    CORRECTIONCOMPLETEDDATETIME TIMESTAMP NOT NULL,
    CORRECTIONRESPONSIBLEWORKER BIGINT   NOT NULL,
    DIAGNOSTICTYPEID           STRING   NOT NULL,
    INVENTNONCONFORMANCEID     STRING   NOT NULL,
    PLANNEDDATE                TIMESTAMP NOT NULL,
    PRIORITY                   INT      NOT NULL,
    REQUESTEDDATE              TIMESTAMP NOT NULL,
    SHORTTERMCORRECTION        INT      NOT NULL,
    COMPANY                    STRING   NOT NULL,
    PARTITION                  STRING   NOT NULL,
    DATAAREAID                 STRING   NOT NULL,
    SYNCSTARTDATETIME         TIMESTAMP NOT NULL,
    RECID                      BIGINT   NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
