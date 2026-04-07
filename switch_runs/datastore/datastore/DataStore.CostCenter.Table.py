# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.CostCenter.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.CostCenter.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the CostCenter table in Databricks under the specified catalog and schema.
# ------------------------------------------------------------------
# The original T‑SQL script defines a standard relational table with
# BIGINT and NVARCHAR data types.  
# In Spark SQL:
#   • BIGINT   → LONG
#   • NVARCHAR → STRING  (Spark stores all variable‑length text as STRING)
#
# ON [PRIMARY] and the SET options are not applicable in Databricks, so
# they are omitted.
# ------------------------------------------------------------------

spark.sql("""
CREATE TABLE `dbe_dbx_internships`.`datastore`.`CostCenter` (
    CostCenterId    LONG   NOT NULL,
    CostCenterCode  STRING NOT NULL,
    CostCenterName  STRING NOT NULL,
    CostCenterCodeName STRING NOT NULL,
    DimensionName   STRING NOT NULL
)
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: verify creation by displaying the schema of the new table
# ------------------------------------------------------------------
spark.sql("DESCRIBE `dbe_dbx_internships`.`datastore`.`CostCenter`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
