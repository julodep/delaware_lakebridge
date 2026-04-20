# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIMainAccountStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIMainAccountStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the Delta table that matches the T‑SQL definition of
# dbo.SMRBIMainAccountStaging.
#
# NOTE:
#   * All object names are fully qualified with dbe_dbx_internships and dbo.
#   * T‑SQL types have been mapped to Spark SQL types:
#       NVARCHAR  -> STRING
#       INT       -> INT
#       BIGINT    -> LONG
#       DATETIME  -> TIMESTAMP
#   * PRIMARY KEY and clustered index clauses are ignored because
#     Delta Lake does not directly support these.  If you require
#     uniqueness enforcement you should add a unique constraint
#     at the application level or use a Delta checkpoint/merge.
#
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIMainAccountStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT      NOT NULL,
    TRANSFERSTATUS INT      NOT NULL,
    CHARTOFACCOUNTSRECID BIGINT NOT NULL,
    MAINACCOUNTCATEGORY STRING NOT NULL,
    MAINACCOUNTID STRING NOT NULL,
    NAME          STRING NOT NULL,
    MAINACCOUNTTYPE INT      NOT NULL,
    MAINACCOUNTRECID BIGINT NOT NULL,
    PARTITION     STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID         BIGINT NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
