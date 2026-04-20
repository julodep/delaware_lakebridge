# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIsmmActivityParentLinkTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIsmmActivityParentLinkTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table `SMRBIsmmActivityParentLinkTableStaging`
# in the specified catalog and schema.  
# All object references are fully‑qualified and use backticks to
# avoid any special characters.
# The mapping of T‑SQL types to Spark SQL types is applied:
#   NVARCHAR -> STRING
#   INT      -> INT
#   BIGINT   -> LONG
#   DATETIME -> TIMESTAMP
# ------------------------------------------------------------------

# Drop the table if it already exists so that the script can be run
# multiple times without errors.
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIsmmActivityParentLinkTableStaging`;")

# COMMAND ----------

# Create the table with the same column definitions as the original
# T‑SQL CREATE TABLE statement, adding NOT NULL constraints.
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIsmmActivityParentLinkTableStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ACTIVITYNUMBER STRING NOT NULL,
    COMPANY STRING NOT NULL,
    PARENTTYPE INT NOT NULL,
    PRIMARYLINK INT NOT NULL,
    SMMACTIVITYPARENTLINKTABLERECID LONG NOT NULL,
    SMMACTIVITYPARENTLINKTABLERECVERSION INT NOT NULL,
    REFRECID LONG NOT NULL,
    REFTABLEID INT NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
