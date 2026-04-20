# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIReqPlanVersionStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIReqPlanVersionStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the staging table in the target Unity Catalog database.
#  The statement maps the original T‑SQL schema to Spark SQL data types.
#  Postgre … (comment: Delta Lake does not support primary‑key enforcement,
#  so the PK definition is omitted; if a unique constraint is required
#  you can add it later using Delta constraints or by application logic).
# ------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIReqPlanVersionStaging` (
    DEFINITIONGROUP        STRING  NOT NULL,
    EXECUTIONID            STRING  NOT NULL,
    ISSELECTED             INT     NOT NULL,
    TRANSFERSTATUS         INT     NOT NULL,
    ACTIVE                 INT     NOT NULL,
    REQPLANVERSIONRECID    BIGINT  NOT NULL,
    REQPLANDATAAREAID      STRING  NOT NULL,
    REQPLANID              STRING  NOT NULL,
    PARTITION              STRING  NOT NULL,
    SYNCSTARTDATETIME      TIMESTAMP NOT NULL,
    RECID                  BIGINT  NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
