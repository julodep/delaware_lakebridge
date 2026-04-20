# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWrkCtrCapResStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWrkCtrCapResStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# 1️⃣  Create the staging table in the specified catalog & schema
# -------------------------------------------------------------
# NOTE: 
#     * All identifiers are fully‑qualified using the syntax
#       `dbe_dbx_internships`.`dbo`.`SMRBIWrkCtrCapResStaging`.
#     * T‑SQL specific options such as PRIMARY KEY clauses and
#       partitioning hints are not supported in Delta Lake via
#       Spark SQL, so they are omitted.  If you need a unique
#       constraint you can enforce it through validation logic
#       or by creating a secondary index with an external service.
#     * Data‑type mappings:
#         - NVARCHAR → STRING
#         - DATETIME/TS → TIMESTAMP
#         - NUMERIC(p,s) → DECIMAL(p,s)
#         - BIGINT, INT, ... are the same in Spark.

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWrkCtrCapResStaging`
(
  `DEFINITIONGROUP`               STRING   NOT NULL,
  `EXECUTIONID`                   STRING   NOT NULL,
  `ISSELECTED`                    INT      NOT NULL,
  `TRANSFERSTATUS`                INT      NOT NULL,
  `COMPANY`                       STRING   NOT NULL,
  `ENDTIME`                       INT      NOT NULL,
  `REFID`                         STRING   NOT NULL,
  `REFTYPE`                       INT      NOT NULL,
  `STARTTIME`                     INT      NOT NULL,
  `TRANSDATE`                     TIMESTAMP NOT NULL,
  `WRKCTRID`                      STRING   NOT NULL,
  `WRKCTRLOADPCT`                 DECIMAL(32,6) NOT NULL,
  `WRKCTRSEC`                     DECIMAL(32,6) NOT NULL,
  `PLANVERSION`                   BIGINT   NOT NULL,
  `PARTITION`                     STRING   NOT NULL,
  `DATAAREAID`                    STRING   NOT NULL,
  `SYNCSTARTDATETIME`             TIMESTAMP NOT NULL,
  `RECID`                         BIGINT   NOT NULL
)
USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
