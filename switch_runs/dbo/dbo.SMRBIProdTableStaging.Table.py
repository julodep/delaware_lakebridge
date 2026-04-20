# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProdTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProdTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create a fully‑qualified Delta Lake table that mirrors the T‑SQL
# definition of [dbo].[SMRBIProdTableStaging].
# ------------------------------------------------------------------
# 1.  Use the T‑SQL column list and convert data types to Spark SQL types:
#     • NVARCHAR(n)  → STRING
#     • NUMERIC(p,s) → DECIMAL(p,s)
#     • DATETIME    → TIMESTAMP
#     • INT         → INT
#     • BIGINT      → LONG
#
# 2.  Spark SQL / Delta Lake does not enforce primary keys natively.
#     We set a primary‑key property so that any downstream tooling
#     that recognises it can still reference the intended uniqueness
#     rule.
#
# 3.  Fully‑qualified names are required: dbe_dbx_internships.dbo.{object_name}.
#
# 4.  The table is created with `USING DELTA` so it becomes a managed
#     Delta table in Databricks.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProdTableStaging` (
    BOMID STRING NOT NULL,
    COLLECTREFPRODID STRING NOT NULL,
    INVENTDIMID STRING NOT NULL,
    INVENTTRANSID STRING NOT NULL,
    ITEMID STRING NOT NULL,
    PRODID STRING NOT NULL,
    ROUTEID STRING NOT NULL,
    DEFAULTDIMENSIONDISPLAYVALUE STRING NOT NULL,
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    COMPANY STRING NOT NULL,
    DENSITY DECIMAL(32,6) NOT NULL,
    DEPTH DECIMAL(32,6) NOT NULL,
    DLVDATE TIMESTAMP NOT NULL,
    FINISHEDDATE TIMESTAMP NOT NULL,
    HEIGHT DECIMAL(32,6) NOT NULL,
    INVENTREFID STRING NOT NULL,
    INVENTREFTYPE INT NOT NULL,
    PRODSTATUS INT NOT NULL,
    QTYCALC DECIMAL(32,6) NOT NULL,
    QTYSCHED DECIMAL(32,6) NOT NULL,
    QTYSTUP DECIMAL(32,6) NOT NULL,
    REALDATE TIMESTAMP NOT NULL,
    REMAININVENTPHYSICAL DECIMAL(32,6) NOT NULL,
    REQPOID STRING NOT NULL,
    SCHEDDATE TIMESTAMP NOT NULL,
    SCHEDEND TIMESTAMP NOT NULL,
    SCHEDSTART TIMESTAMP NOT NULL,
    SCHEDSTATUS INT NOT NULL,
    STUPDATE TIMESTAMP NOT NULL,
    WIDTH DECIMAL(32,6) NOT NULL,
    PRODTABLEDIMENSION LONG NOT NULL,
    PRODPOOLID STRING NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.primaryKey'='PRODID,EXECUTIONID,PARTITION'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
