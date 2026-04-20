# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWrkCtrActivityRequirementStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWrkCtrActivityRequirementStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------
# Create the Delta table `SMRBIWrkCtrActivityRequirementStaging`
# in the specified Unity Catalog
# -------------------------------------------------------

# Drop the table if it already exists to keep the script idempotent
spark.sql(
    f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIWrkCtrActivityRequirementStaging`"
)

# COMMAND ----------

# Create the table with the data types translated to Spark SQL
#   - NVARCHAR -> STRING
#   - INT      -> INT
#   - BIGINT   -> BIGINT
#   - DATETIME -> TIMESTAMP
# Primary‑key and other T‑SQL constraints are not directly supported in Delta Lake,
# so they are omitted.  If required, uniqueness can be enforced at query time
# or via a separate constraint mechanism.
spark.sql(
    f"""
    CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWrkCtrActivityRequirementStaging` (
        DEFINITIONGROUP STRING NOT NULL,
        EXECUTIONID STRING NOT NULL,
        ISSELECTED INT NOT NULL,
        TRANSFERSTATUS INT NOT NULL,
        ACTIVITYREQUIREMENTSET BIGINT NOT NULL,
        WRKCTRACTIVITYREQUIREMENTRECID BIGINT NOT NULL,
        PARTITION STRING NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL,
        RECID BIGINT NOT NULL
    )
    USING DELTA
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
