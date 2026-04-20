# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDeliveryModeStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDeliveryModeStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------
# Create the staged delivery‑mode table in Unity Catalog
# --------------------------------------------
# The target database objects must use fully‑qualified names:
#   dbe_dbx_internships.dbo.SMRBIDeliveryModeStaging
#
# Data‑type mapping (T‑SQL → Spark SQL):
#   NVARCHAR(n)   -> STRING
#   INT           -> INT
#   DATETIME      -> TIMESTAMP
#
# The PRIMARY KEY definition and index hints are ignored because Delta
# tables do not support explicit key constraints.  If you need a unique
# enforcement you can add a `SET CONSTRAINTS` statement afterwards
# (not shown here).

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIDeliveryModeStaging` (
        DEFINITIONGROUP   STRING NOT NULL,
        EXECUTIONID       STRING NOT NULL,
        ISSELECTED        INT    NOT NULL,
        TRANSFERSTATUS    INT    NOT NULL,
        MODECODE          STRING NOT NULL,
        MODEDESCRIPTION   STRING NOT NULL,
        COMPANY           STRING NOT NULL,
        PARTITION         STRING NOT NULL,
        DATAAREAID        STRING NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
