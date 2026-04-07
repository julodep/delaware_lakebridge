# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Batch.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Batch.Table.sql`

# COMMAND ----------

# -----------------------------------------------------------------------------------------------
# NOTE: This notebook creates the `Batch` table in the specified catalog and schema.
#       All references are fully qualified as dbe_dbx_internships.datastore.Batch.
#
#       The following T‑SQL session settings (ANSI_NULLS, QUOTED_IDENTIFIER) are ignored by Databricks
#       Spark and therefore commented out.  They are included here only for completeness
#       with the original script.
# -----------------------------------------------------------------------------------------------

# T‑SQL session settings are not applicable in Databricks; we simply comment them.
# SET ANSI_NULLS ON
# GO
# SET QUOTED_IDENTIFIER ON
# GO

# Create the table using Spark SQL.  Use the compatible data types:
#   BIGINT   -> LONG
#   NVARCHAR -> STRING
#   DATETIME -> TIMESTAMP
# The NVARCHAR(MAX) column is represented as STRING in Spark.
spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Batch` (
        RecId          LONG,
        CompanyCode    STRING,
        BatchCode      STRING,
        ProductCode    STRING,
        Description    STRING,
        ExpiryDate     TIMESTAMP,
        ProductionDate TIMESTAMP
    )
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
