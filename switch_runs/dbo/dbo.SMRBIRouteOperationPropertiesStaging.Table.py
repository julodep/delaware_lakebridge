# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIRouteOperationPropertiesStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIRouteOperationPropertiesStaging.Table.sql`

# COMMAND ----------

# Create the staging table as a Delta table in the specified catalog and schema.
# Column types are mapped from T‑SQL to Spark SQL. 
# Delta Lake does not support PRIMARY KEY constraints; the original key is commented out for documentation purposes.

spark.sql(
"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIRouteOperationPropertiesStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID STRING,
    ISSELECTED INT,
    TRANSFERSTATUS INT,
    SETUPCOSTCATEGORYID STRING,
    QUANTITYCOSTCATEGORYID STRING,
    OPERATIONID STRING,
    PRODUCTIONSITEID STRING,
    PROCESSCOSTCATEGORYID STRING,
    PROCESSTIME DECIMAL(32,6),
    ROUTEGROUPID STRING,
    ROUTEID STRING,
    SETUPTIME DECIMAL(32,6),
    COSTINGOPERATIONRESOURCEID STRING,
    COMPANY STRING,
    PARTITION STRING,
    DATAAREAID STRING,
    SYNCSTARTDATETIME TIMESTAMP,
    RECID LONG
)
USING DELTA
-- PRIMARY KEY definition (removed: Delta Lake does not enforce primary keys)
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
