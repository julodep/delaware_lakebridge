# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIReqMasterPlanningParametersStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIReqMasterPlanningParametersStaging.Table.sql`

# COMMAND ----------

# Define default catalog and schema if they are not already defined
try:
    catalog
except NameError:
    catalog = "default_catalog"

# COMMAND ----------

try:
    schema
except NameError:
    schema = "default_schema"

# COMMAND ----------

# Create the staging table `SMRBIReqMasterPlanningParametersStaging` in the specified Unity Catalog
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIReqMasterPlanningParametersStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID STRING,
    ISSELECTED INT,
    TRANSFERSTATUS INT,
    CURRENTDYNAMICMASTERPLANID STRING,
    COMPANY STRING,
    PARTITION STRING,
    DATAAREAID STRING,
    SYNCSTARTDATETIME TIMESTAMP
)
""")  # Primary key (EXECUTIONID, COMPANY, DATAAREAID, PARTITION) is not enforced in Delta Lake and is omitted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
