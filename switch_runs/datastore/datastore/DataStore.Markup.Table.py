# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Markup.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Markup.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Databricks notebook – Create the Markup dimension table
# -------------------------------------------------------------
# This notebook recreates the T‑SQL `CREATE TABLE` statement
# for the `Markup` table in the `DataStore` database.
# All catalog/schema/object references are fully‑qualified
# using the placeholders `dbe_dbx_internships` and `datastore`.
#
# The original T‑SQL also contains session settings
# (`SET ANSI_NULLS ON`, `SET QUOTED_IDENTIFIER ON`) which
# are specific to SQL Server and have no effect in Databricks.
# They are therefore omitted here.
#
# Column definitions:
#   CompanyCode      → STRING (nullable)
#   TransRecId       → BIGINT NOT NULL
#   MarkupCategory   → INT   NOT NULL
#   TransTableCode   → INT   NOT NULL
#   SurchargeTransport   → DECIMAL(32,6) NOT NULL
#   SurchargePurchase   → DECIMAL(32,6) NOT NULL
#   SurchargeDelivery   → DECIMAL(32,6) NOT NULL
#   SurchargeTotal       → DECIMAL(34,6)  (nullable)
#
# In Delta Lake we can specify `NOT NULL` constraints on columns,
# so we keep those constraints in the table definition.
#
# The table is created as a Delta table so that future
# transformations or overwrites work seamlessly.
#
# -------------------------------------------------------------
spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Markup` (
        CompanyCode STRING,
        TransRecId BIGINT NOT NULL,
        MarkupCategory INT NOT NULL,
        TransTableCode INT NOT NULL,
        SurchargeTransport DECIMAL(32,6) NOT NULL,
        SurchargePurchase DECIMAL(32,6) NOT NULL,
        SurchargeDelivery DECIMAL(32,6) NOT NULL,
        SurchargeTotal DECIMAL(34,6)
    )
    """
)

# COMMAND ----------

# -------------------------------------------------------------
# Verify the schema was created correctly
# -------------------------------------------------------------
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`datastore`.`Markup`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
