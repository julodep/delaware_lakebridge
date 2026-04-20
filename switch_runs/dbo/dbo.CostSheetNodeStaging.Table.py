# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.CostSheetNodeStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.CostSheetNodeStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the CostSheetNodeStaging table in the target catalog and schema
# ------------------------------------------------------------------
#
# The original T‑SQL statement defines a table with many columns and a
# composite PRIMARY KEY.  Delta Lake (the engine used by Databricks)
# does not support primary‑key or clustered‑index definitions, so we
# create the table with the same column definitions and type mappings
# but comment out the unsupported constraints.  Spark SQL data types
# are used:
#
#   NVARCHAR(n)  -> STRING
#   INT          -> INT
#   DATETIME     -> TIMESTAMP
#
# The fully‑qualified object name must use the format
# `dbe_dbx_internships`.`dbo`.`CostSheetNodeStaging`.
# ------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`CostSheetNodeStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ISCALCULATIONFACTORSPECIFIEDPERITEM INT NOT NULL,
    RATENODESUBTYPE INT NOT NULL,
    SURCHARGENODESUBTYPE INT NOT NULL,
    UNITBASEDNODESUBTYPE INT NOT NULL,
    NODENAME STRING NOT NULL,
    COSTGROUPID STRING NOT NULL,
    NODEDESCRIPTION STRING NOT NULL,
    ISNODESHOWNASHEADER INT NOT NULL,
    PRICENODESUBTYPE INT NOT NULL,
    ISNODESHOWNASTOTALLINE INT NOT NULL,
    NODETYPE INT NOT NULL,
    DEFAULTLEDGERDIMENSIONDISPLAYVALUE STRING NOT NULL,
    ESTIMATEDINDIRECTABSORPTIONMAINACCOUNTIDDISPLAYVALUE STRING NOT NULL,
    ESTIMATEDINDIRECTABSORPTIONOFFSETMAINACCOUNTIDDISPLAYVALUE STRING NOT NULL,
    INDIRECTABSORPTIONMAINACCOUNTIDDISPLAYVALUE STRING NOT NULL,
    INDIRECTABSORPTIONOFFSETMAINACCOUNTIDDISPLAYVALUE STRING NOT NULL,
    PARENTNODENAME STRING NOT NULL,
    ABSORPTIONBASISNODENAME STRING NOT NULL,
    SURCHARGENODEABSORPTIONBASISSUBTYPE INT NOT NULL,
    RATENODEABSORPTIONBASISSUBTYPE INT NOT NULL,
    PARTITION STRING NOT NULL,
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
