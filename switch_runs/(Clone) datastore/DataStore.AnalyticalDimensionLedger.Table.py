# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AnalyticalDimensionLedger.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastoredemo/DataStore.AnalyticalDimensionLedger.Table.sql`

# COMMAND ----------

# --------------------------------------------------
# Create a permanent Delta table with all columns
# that were defined in the original T‑SQL statement.
# --------------------------------------------------
# Every reference is fully‑qualified in the form
# dbe_dbx_internships.datastore.{table_name}.  Replace {catalog}
# and {schema} with the appropriate names when you
# execute the notebook.
# --------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE DataStore.`AnalyticalDimensionLedger` (
    LedgerDimensionId BIGINT NOT NULL,
    MainAccount       BIGINT NOT NULL,
    Intercompany      BIGINT NOT NULL,
    BusinessSegment   BIGINT NOT NULL,
    EndCustomer       BIGINT NOT NULL,
    Department        BIGINT NOT NULL,
    LocalAccount      BIGINT NOT NULL,
    Location          BIGINT NOT NULL,
    Product           BIGINT NOT NULL,
    ShipmentContract  BIGINT NOT NULL,
    Vendor            BIGINT NOT NULL
)
""")

# COMMAND ----------

# --------------------------------------------------
# Optional: Verify the table was created by listing its schema.
# --------------------------------------------------
df = spark.table(f"{'dbe_dbx_internships'.format(catalog='dbe_dbx_internships')}.DataStore.AnalyticalDimensionLedger")
print("Schema of AnalyticalDimensionLedger:")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
