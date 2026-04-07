# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.CostPrice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.CostPrice.Table.sql`

# COMMAND ----------

# -------------------------------------------------------
#  Create the CostPrice table in the target catalog & schema
#  -------------------------------------------------------
#  The original T‑SQL statement was created in the [DataStore] schema.
#  For Databricks we are required to use the fully‑qualified form
#  dbe_dbx_internships.datastore.CostPrice – the dbe_dbx_internships and datastore placeholders
#  will be replaced by the runtime‑specific values before execution.
#
#  Mapping of data types:
#    nvarchar(20)        -> STRING
#    nvarchar(10)        -> STRING
#    nvarchar(4)         -> STRING
#    numeric(38,9)       -> DECIMAL(38,9)
#    datetime            -> TIMESTAMP
#
#  In Spark / Delta Lake  we do **not** support NOT NULL constraints
#  in the SQL CREATE statement, so the OPTIONAL behaviour remains
#  unchanged – the column will simply allow NULLs by default, but
#  we keep the comment to document the intended semantics.
#
spark.sql("""
CREATE TABLE `dbe_dbx_internships`.`datastore`.`CostPrice` (
    ItemNumber         STRING NOT NULL,
    InventDimCode      STRING NOT NULL,
    UnitCode           STRING NOT NULL,
    CompanyCode        STRING NOT NULL,
    Price              DECIMAL(38,9),
    StartValidityDate  TIMESTAMP NOT NULL,
    EndValidityDate    TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# Optional: inspect the newly created table's schema
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`datastore`.`CostPrice`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
