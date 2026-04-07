# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.EndCustomer.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.EndCustomer.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the `EndCustomer` table in the target Unity Catalog
#  ---------------------------------------------------------
#  • All names are fully‑qualified: `dbe_dbx_internships`.`datastore`.`EndCustomer`
#  • Data types are mapped from T‑SQL to Spark SQL:
#        BIGINT      -> LONG
#        NVARCHAR(..)-> STRING
#  • The NOT NULL constraints are preserved.
#  • We use DELTA as the storage format (the default for Unity Catalog tables).
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`EndCustomer`
(
  EndCustomerId        LONG  NOT NULL,
  EndCustomerCode      STRING NOT NULL,
  EndCustomerName      STRING NOT NULL,
  EndCustomerCodeName  STRING NOT NULL,
  DimensionName        STRING NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
