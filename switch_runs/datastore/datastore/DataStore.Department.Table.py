# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Department.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Department.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Convert T‑SQL CREATE TABLE to Databricks Delta Lake table
# ------------------------------------------------------------------
#
#  - T‑SQL uses [DataStore].[Department] – in Spark we need a fully
#    qualified name: `dbe_dbx_internships`.`datastore`.`Department`.
#  - NVARCHAR maps to STRING in Spark SQL.
#  - NOT NULL constraints are expressed with the NOT NULL keyword.
#  - The table is created as a Delta table so it can be queried
#    efficiently and persisted in Unity Catalog.
#
#  Note: The MS‑SQL option `ON [PRIMARY]` is omitted because
#  partitions are not defined yet.  You may add a partition specification
#  later if required.
#
# ------------------------------------------------------------------
spark.sql(
    f"""
    CREATE TABLE `dbe_dbx_internships`.`datastore`.`Department` (
        DepartmentId      BIGINT   NOT NULL,
        DepartmentCode    STRING   NOT NULL,
        DepartmentName    STRING   NOT NULL,
        DepartmentCodeName STRING NOT NULL,
        DimensionName     STRING   NOT NULL
    )
    """
)

# COMMAND ----------

# ---- OPTIONAL: Verify creation by showing the schema ----------------
display(spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`datastore`.`Department`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
