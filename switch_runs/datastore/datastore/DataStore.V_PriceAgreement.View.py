# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_PriceAgreement.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_PriceAgreement.View.sql`

# COMMAND ----------

# -----------------------------------------------------------
# Databricks notebook – Create persistent view
# -----------------------------------------------------------
# All objects are fully‑qualified with the target catalog and schema.
# The original T‑SQL view [DataStore].[V_PriceAgreement] is translated into
# a Spark SQL view named the same way but without the square‑bracket
# syntax.  The only functional change required is the handling of the
# `VendorCode` column where an empty string is converted to the
# placeholder '_N/A'.  In Spark SQL this is achieved with
#   COALESCE(NULLIF(AccountRelation, ''), '_N/A')
# -----------------------------------------------------------

# Replace dbe_dbx_internships and datastore with your Unity Catalog values
# e.g. `spark_catalog`, `DataStore`
catalog_name = "dbe_dbx_internships"
schema_name  = "datastore"   # e.g. 'DataStore'

# COMMAND ----------

# Full view name
view_name = f"`{catalog_name}`.`{schema_name}`.`V_PriceAgreement`"

# COMMAND ----------

# Full source table name
source_table = f"`{catalog_name}`.`{schema_name}`.`SMRBIPriceDiscTableStaging`"

# COMMAND ----------

# Create or replace the persistent view
spark.sql(f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT
    /* VendorCode: coalesce empty string to '_N/A' – same as ISNULL(NULLIF(...)) */
    COALESCE(NULLIF(AccountRelation, ''), '_N/A') AS VendorCode,
    /* Direct column mappings with explicit aliases */
    ItemRelation   AS ProductCode,
    DataAreaId     AS CompanyCode,
    Amount,
    Currency,
    FromDate,
    ToDate,
    QuantityAmountFrom AS QtyFrom,
    QuantityAmountTo   AS QtyTo,
    UnitId,
    PriceUnit
FROM {source_table} AS P
/* The WHERE 1=1 in the original script is a no‑op, so we keep only the real filter */
WHERE Module = 2
""")

# COMMAND ----------

# Verify that the view exists (optional – for debugging purposes)
spark.catalog.listViews().filter(f"viewName == 'V_PriceAgreement'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
