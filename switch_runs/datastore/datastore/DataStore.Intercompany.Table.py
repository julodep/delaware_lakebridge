# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Intercompany.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Intercompany.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook – Creation of the Intercompany dimension table
# ------------------------------------------------------------------
# The original T‑SQL statement:
#     CREATE TABLE [DataStore].[Intercompany] ( ... ) ON [PRIMARY]
#
#   1. All identifiers are converted to fully‑qualified names:
#          dbe_dbx_internships.datastore.Intercompany
#   2. Data types are mapped to Spark SQL types:
#          BIGINT  -> LONG
#          NVARCHAR -> STRING
#   3. The optional storage clause “ON [PRIMARY]” is ignored – Delta
#      tables are automatically placed in the default storage location.
#   4. We use a standard `CREATE TABLE ...` statement that will
#      overwrite an existing table if it already exists, because
#      Delta Lake is more tolerant of idempotent creations.
#
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Intercompany` (
    IntercompanyId   LONG NOT NULL,
    IntercompanyCode STRING NOT NULL,
    IntercompanyName STRING NOT NULL,
    IntercompanyCodeName STRING NOT NULL,
    DimensionName    STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
