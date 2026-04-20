# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustomerPriorityClassificationGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustomerPriorityClassificationGroupStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# Create the staging table in the target Unity Catalog database.
# --------------------------------------------------------------
# Delta Lake (the underlying format in Databricks) does not support
# primary‑key enforcement or clustered indexes directly, so we create
# the table with the same columns and note the intended uniqueness
# constraint in the comment below. If constraint enforcement is
# required the application layer should check for duplicates before
# inserting.
# --------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBICustomerPriorityClassificationGroupStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CUSTOMERPRIORITYCLASSIFICATIONGROUPCODE STRING NOT NULL,
    DESCRIPTION STRING NOT NULL,
    COMPANY STRING NOT NULL,
    `PARTITION` STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
