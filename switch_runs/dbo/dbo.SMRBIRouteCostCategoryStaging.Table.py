# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIRouteCostCategoryStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIRouteCostCategoryStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the SMRBIRouteCostCategoryStaging table in Databricks
# ------------------------------------------------------------------
# The original T‑SQL statement defines a regular table with a primary key
# and a handful of NOT NULL constraints.  Delta tables in Spark do not
# have a built‑in PRIMARY KEY definition, so the constraint is omitted
# (but the columns required for uniqueness are still present).
#
# All identifiers are fully‑qualified:  dbe_dbx_internships.dbo.SMRBIRouteCostCategoryStaging
# The data‑type mapping follows the guidelines:  NVARCHAR → STRING, INT → INT,
# NUMERIC(p, s) → DECIMAL(p, s).  No additional indexes are created.
#
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIRouteCostCategoryStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CATEGORYID STRING NOT NULL,
    COSTGROUPID STRING NOT NULL,
    UNITCOST DECIMAL(32, 6) NOT NULL,
    ABSORBEDMANUFACTURINGCOSTMAINACCOUNTIDDISPLAYVALUE STRING NOT NULL,
    ABSORBEDWIPMANUFACTURINGCOSTMAINACCOUNTIDDISPLAYVALUE STRING NOT NULL,
    ESTIMATEDABSORBEDMANUFACTURINGCOSTMAINACCOUNTIDDISPLAYVALUE STRING NOT NULL,
    ESTIMATEDABSORBEDWIPMANUFACTURINGCOSTMAINACCOUNTIDDISPLAYVALUE STRING NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
