# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductFD.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ProductFD.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the table `ProductFD` inside the specified catalog & schema
# ------------------------------------------------------------------
# • T‑SQL objects are referenced with brackets (e.g. [DataStore].[ProductFD]).
# • In Databricks we use the fully‑qualified format:
#     `dbe_dbx_internships`.`datastore`.`ProductFD`
# • Data types:
#     BIGINT → BIGINT (Spark SQL supports BIGINT natively)
#     NVARCHAR → STRING
# • The `SET ANSI_NULLS` / `SET QUOTED_IDENTIFIER` statements have no
#   effect in Databricks, so they are omitted.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`ProductFD` (
    ProductFDId BIGINT,
    ProductFDCode STRING,
    ProductFDName STRING,
    ProductFDCodeName STRING,
    DimensionName STRING
)
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Verify the table exists (optional)
# ------------------------------------------------------------------
df = spark.table(f"`dbe_dbx_internships`.`datastore`.`ProductFD`")
print("Table schema:")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
