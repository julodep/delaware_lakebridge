# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWHSPhysicalDimensionGroupDetailStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWHSPhysicalDimensionGroupDetailStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Databricks notebook – Create the table `SMRBIWHSPhysicalDimensionGroupDetailStaging`
#  in the specified catalog and schema.
#
#  Input T‑SQL (shortened):
#      CREATE TABLE [dbo].[SMRBIWHSPhysicalDimensionGroupDetailStaging] ( ... )
#
#  Databricks rules:
#    • Use fully‑qualified names: dbe_dbx_internships.dbo.{object_name}
#    • Delta Lake (the default storage format in Databricks) does not support
#      primary‑key or clustered‑index constraints. We therefore define the
#      table without them and add a comment explaining the omission.
#    • Data‑type mapping:
#        NVARCHAR(n)          -> STRING
#        NUMERIC(p,s)         -> DECIMAL(p,s)
#        BIGINT               -> BIGINT
#        INT                  -> INT
#        DATETIME             -> TIMESTAMP
#    • The column names are kept identical to the source, but the
#      surrounding brackets and quotes are removed because Spark SQL uses
#      backticks for identifiers when needed.
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# 1. Define the full name of the table.
# ------------------------------------------------------------------
table_name = f"`dbe_dbx_internships`.`dbo`.`SMRBIWHSPhysicalDimensionGroupDetailStaging`"

# COMMAND ----------

# ------------------------------------------------------------------
# 2. Create the table with the correct schema.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE {table_name} (
    DEFINITIONGROUP        STRING      NOT NULL,
    EXECUTIONID            STRING      NOT NULL,
    ISSELECTED              INT       NOT NULL,
    TRANSFERSTATUS          INT       NOT NULL,
    PHYSICALDEPTH        DECIMAL(32,6) NOT NULL,
    PHYSICALHEIGHT       DECIMAL(32,6) NOT NULL,
    PHYSICALDIMENSIONGROUPID STRING   NOT NULL,
    PHYSICALUNITSYMBOL    STRING      NOT NULL,
    PHYSICALWEIGHT       DECIMAL(32,12) NOT NULL,
    PHYSICALWIDTH        DECIMAL(32,6) NOT NULL,
    COMPANY               STRING      NOT NULL,
    PARTITION              STRING      NOT NULL,
    DATAAREAID             STRING      NOT NULL,
    SYNCSTARTDATETIME     TIMESTAMP   NOT NULL,
    RECID                 BIGINT      NOT NULL
)
USING delta  -- Default format in Databricks
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
