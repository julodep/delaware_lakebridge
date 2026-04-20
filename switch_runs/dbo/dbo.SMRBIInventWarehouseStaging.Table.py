# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventWarehouseStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventWarehouseStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create a Delta‑Lake table that mirrors the T‑SQL staging table
#   dbo.SMRBIInventWarehouseStaging
#
# All object references use the fully‑qualified format
#   `dbe_dbx_internships`.`dbo`.`SMRBIInventWarehouseStaging`
#
# NOTE:
#   • T‑SQL primary‑key and clustering constraints cannot be expressed
#     directly in Delta Lake.  They are omitted here; data quality
#     rules (unique key on EXECUTIONID/WAREHOUSEID/DATAAREAID/PARTITION)
#     can be enforced downstream if required.
#   • The column data types have been converted to Spark SQL data
#     types:
#       NVARCHAR   → STRING
#       INT        → INT
#       DATETIME   → TIMESTAMP
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventWarehouseStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    WAREHOUSEID   STRING NOT NULL,
    WAREHOUSENAME STRING NOT NULL,
    COMPANY       STRING NOT NULL,
    PARTITION     STRING NOT NULL,
    DATAAREAID    STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# You can verify the schema after creation:
spark.table(f"dbe_dbx_internships.dbo.SMRBIInventWarehouseStaging").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
