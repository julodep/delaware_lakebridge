# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventProductGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventProductGroupStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create staging table SMRBIInventProductGroupStaging
#  This script translates the T‑SQL CREATE TABLE statement to
#  Databricks Delta Lake / Spark SQL.  
#  - All identifiers are fully‑qualified: `dbe_dbx_internships`.`dbo`.`SMRBIInventProductGroupStaging`.
#  - Spark SQL does not support PRIMARY KEY constraints or the extended
#    ON [PRIMARY] clause; therefore the constraint is omitted and a
#    comment explains the omission.
#  - VARCHAR(n) and NVARCHAR(n) are mapped to Spark STRING.
#  - INT  -> IntegerType (INT), BIGINT -> BigIntType, DATETIME -> TIMESTAMP.
# -----------------------------------------------------------------

# Drop the table if it already exists to avoid compilation errors.
spark.sql(
    f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIInventProductGroupStaging`"
)

# COMMAND ----------

# Create the identical schema in Delta Lake.
spark.sql(
    f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventProductGroupStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT     NOT NULL,
    TRANSFERSTATUS INT    NOT NULL,
    ITEMGROUPID   STRING  NOT NULL,
    NAME          STRING  NOT NULL,
    INVENTPRODUCTRECID BIGINT NOT NULL,
    COMPANY       STRING  NOT NULL,
    PARTITION     STRING  NOT NULL,
    DATAAREAID    STRING  NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
