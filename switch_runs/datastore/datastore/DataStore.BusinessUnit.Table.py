# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.BusinessUnit.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.BusinessUnit.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# Create the `BusinessUnit` table in the target catalog and schema.
# --------------------------------------------------------------------
# The original T‑SQL statement created a table in database `DataStore`
# with the following columns:
#   * BusinessUnitId          BIGINT      NOT NULL
#   * BusinessUnitCode        NVARCHAR(30) NOT NULL
#   * BusinessUnitName        NVARCHAR(60) NOT NULL
#   * BusinessUnitCodeName    NVARCHAR(91) NOT NULL
#   * DimensionName           NVARCHAR(60) NOT NULL
#
# In Spark/Databricks the equivalent data‑types are:
#   * BIGINT   -> LONG
#   * NVARCHAR -> STRING
#
# NOTE: Spark Delta tables do not enforce NOT NULL constraints by
# default; the `NOT NULL` clauses are kept only for readability.
#
spark.sql(
    """
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`BusinessUnit` (
        BusinessUnitId   LONG   NOT NULL,
        BusinessUnitCode STRING NOT NULL,
        BusinessUnitName STRING NOT NULL,
        BusinessUnitCodeName STRING NOT NULL,
        DimensionName   STRING NOT NULL
    )
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
