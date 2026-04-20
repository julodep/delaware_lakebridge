# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIOpResOperationsResourceGroupAssignmentStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIOpResOperationsResourceGroupAssignmentStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create staging table "SMRBIOpResOperationsResourceGroupAssignmentStaging"
# in dbe_dbx_internships.dbo with Spark/Delta types
# --------------------------------------------------------------
# All identifiers are fully‑qualified to avoid any catalog/schema ambiguity.
# Data type mapping:
#   NVARCHAR → STRING
#   INT      → INT
#   BIGINT   → BIGINT
#   DATETIME → TIMESTAMP
# --------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIOpResOperationsResourceGroupAssignmentStaging (
    DEFINITIONGROUP      STRING,
    EXECUTIONID          STRING,
    ISSELECTED           INT,
    TRANSFERSTATUS       INT,
    VALIDFROM            TIMESTAMP,
    VALIDTO              TIMESTAMP,
    OPERATIONSRESOURCEID STRING,
    OPERATIONSRESOURCEGROUPID STRING,
    COMPANY              STRING,
    PARTITION            STRING,
    DATAAREAID           STRING,
    SYNCSTARTDATETIME    TIMESTAMP,
    RECID                BIGINT
)
USING DELTA
""")

# COMMAND ----------

# --------------------------------------------------------------
# NOTE:
#   - Primary key constraints are not automatically enforced in Delta Lake.
#   - If enforcement is required, add a Delta Table property:
#       ALTER TABLE ... SET TBLPROPERTIES ("delta.primaryKey"="EXECUTIONID,VALIDFROM,VALIDTO,OPERATIONSRESOURCEID,DATAAREAID,PARTITION");
#   - The original T‑SQL constraint is therefore omitted in the Spark script.
# --------------------------------------------------------------

# If you want to inspect the created table schema:
spark.sql(f"DESCRIBE TABLE dbe_dbx_internships.dbo.SMRBIOpResOperationsResourceGroupAssignmentStaging").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
