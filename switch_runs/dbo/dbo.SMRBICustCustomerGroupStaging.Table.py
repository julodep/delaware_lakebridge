# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustCustomerGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustCustomerGroupStaging.Table.sql`

# COMMAND ----------

# Create the SMRBICustCustomerGroupStaging table in the target catalog and schema
# NOTE: Delta Lake (Databricks) does not support PRIMARY KEY constraints directly.
# The columns that were part of the T‑SQL PK are documented in the COMMENT field.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.SMRBICustCustomerGroupStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CUSTGROUP STRING NOT NULL,
    NAME STRING NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
COMMENT 'Primary key: EXECUTIONID, CUSTGROUP, DATAAREAID, PARTITION'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
