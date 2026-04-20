# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIOpResOperationsResourceGroupWorkCalendarAssignmentStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIOpResOperationsResourceGroupWorkCalendarAssignmentStaging.Table.sql`

# COMMAND ----------

# ----------------------------------------------
# Create the staging table in Unity Catalog
# ----------------------------------------------
# In T‑SQL the table had a cluster‑based primary key and storage options that Spark SQL does not support.
# We create the equivalent table with the same column names and data types and simply omit the
# PRIMARY KEY and ON PRIMARY clause – these are ignored by Databricks.  If you need to enforce
# uniqueness you can later add a CHECK constraint or a validation job.
# ----------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIOpResOperationsResourceGroupWorkCalendarAssignmentStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID STRING,
    ISSELECTED INT,
    TRANSFERSTATUS INT,
    OPERATIONSRESOURCEGROUPID STRING,
    WORKCALENDARID STRING,
    VALIDFROM TIMESTAMP,
    VALIDTO TIMESTAMP,
    COMPANY STRING,
    PARTITION STRING,
    DATAAREAID STRING,
    SYNCSTARTDATETIME TIMESTAMP,
    RECID BIGINT
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
