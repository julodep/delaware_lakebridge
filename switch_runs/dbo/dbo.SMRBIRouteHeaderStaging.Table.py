# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIRouteHeaderStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIRouteHeaderStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
# Create a staging Delta table that mirrors the T‑SQL definition
# --------------------------------------------------------------------------
# 1.  The fully‑qualified object name follows the pattern:
#     dbe_dbx_internships.dbo.SMRBIRouteHeaderStaging
#
# 2.  Data types are mapped from T‑SQL to Spark SQL:
#       NVARCHAR            -> STRING
#       INT                 -> INT
#       DATETIME            -> TIMESTAMP
#
# 3.  Primary‑key and index definitions are not supported in Delta Lake.
#     They are omitted from the DDL.
#
# 4.  The table is created with `USING delta`.
#     Partitioning is optional; here it is done on PARTITION and DATAAREAID.
# --------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIRouteHeaderStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ROUTENAME     STRING NOT NULL,
    ROUTEID       STRING NOT NULL,
    COMPANY       STRING NOT NULL,
    PARTITION     STRING NOT NULL,
    DATAAREAID    STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING delta
PARTITIONED BY (PARTITION, DATAAREAID)
""")

# COMMAND ----------

# --------------------------------------------------------------------------
# Optional: Verify the table was created
# --------------------------------------------------------------------------
spark.sql(
    f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIRouteHeaderStaging`"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
