# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIReqItemCoverageSettingsStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIReqItemCoverageSettingsStaging.Table.sql`

# COMMAND ----------

# ----------------------------------------------------
#  Create the staging table in the target catalog and schema.
#  Data types are mapped from T‑SQL to Spark/Delta Lake types:
#    NVARCHAR → STRING          (TEXT with length limitation ignored)
#    INT      → INT
#    DATETIME → TIMESTAMP
#  Delta Lake does not enforce primary keys or clustering by
#  default, but the UNIQUE‑CONSTRAINT can be documented for
#  reference.
# ----------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIReqItemCoverageSettingsStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT    NOT NULL,
    TRANSFERSTATUS INT    NOT NULL,
    ITEMNUMBER       STRING NOT NULL,
    COVERAGEWAREHOUSEID STRING NOT NULL,
    COVERAGEPRODUCTCONFIGURATIONID STRING NOT NULL,
    VENDORACCOUNTNUMBER STRING NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
COMMENT '' -- Delta Lake does not natively support PRIMARY KEY constraints in the same way SQL Server does.
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
