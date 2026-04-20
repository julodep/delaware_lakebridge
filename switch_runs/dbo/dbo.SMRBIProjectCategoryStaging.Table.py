# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjectCategoryStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjectCategoryStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the staging table `SMRBIProjectCategoryStaging`
# --------------------------------------------------------------
# Note: In Databricks (Delta Lake) primary‑key constraints and
# clustering are not supported the same way they are in SQL Server.
# The primary key defined in the T‑SQL statement is therefore omitted
# and a comment is added to explain that the uniqueness guarantee is
# not enforced at the storage level.  If you need a unique constraint
# you can enforce it in a downstream processing step.
#
# All identifiers are fully‑qualified: dbe_dbx_internships.dbo.SMRBIProjectCategoryStaging
#
# Data type mapping:
#   NVARCHAR → STRING
#   INT      → INT
#   DATETIME → TIMESTAMP
#
# The statement below creates a Delta table with the same columns and
# NOT NULL constraints as the original T‑SQL CREATE TABLE.
# --------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjectCategoryStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT    NOT NULL,
    TRANSFERSTATUS INT    NOT NULL,
    CATEGORY       STRING NOT NULL,
    CATEGORYNAME   STRING NOT NULL,
    COMPANY        STRING NOT NULL,
    PARTITION      STRING NOT NULL,
    DATAAREAID     STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
