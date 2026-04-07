# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ShipmentContract.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ShipmentContract.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the ShipmentContract table in the target Unity Catalog.
# The T‑SQL CREATE TABLE statement is translated to a Spark SQL
# CREATE OR REPLACE TABLE statement with fully‑qualified names.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.ShipmentContract (
    ShipmentContractId LONG NOT NULL,
    ShipmentContractCode STRING NOT NULL,
    ShipmentContractName STRING NOT NULL,
    ShipmentContractCodeName STRING NOT NULL,
    DimensionName STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
