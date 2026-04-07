# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Time.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Time.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create a Delta table in the specified catalog and schema that mirrors
# the T‑SQL table [DataStore].[Time].
#
# T‑SQL snippet (simplified):
#   CREATE TABLE [DataStore].[Time](
#       DimTimeId       INT          NOT NULL,
#       HourId          INT          NOT NULL,
#       HourZoneCode    NVARCHAR(10) NOT NULL,
#       HourZoneName    NVARCHAR(20) NOT NULL,
#       MinuteId        INT          NOT NULL,
#       Time            NVARCHAR(10) NOT NULL
#   ) ON [PRIMARY];
#
# Databricks/Spark SQL uses the following type mapping:
#   INT          -> INT
#   NVARCHAR(n)  -> STRING (length is ignored in Spark)
# We keep the NOT NULL constraints but omit the primary‑key/PRIMARY 
# clause because Delta Lake does not support native clustered or 
# non‑clustered indices in the same way SQL Server does.
# ------------------------------------------------------------------
spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Time` (
        DimTimeId      INT NOT NULL,
        HourId         INT NOT NULL,
        HourZoneCode   STRING NOT NULL,
        HourZoneName   STRING NOT NULL,
        MinuteId       INT NOT NULL,
        `Time`         STRING NOT NULL
    )
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
