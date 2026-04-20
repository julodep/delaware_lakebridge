# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWMSWarehouseLocationStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWMSWarehouseLocationStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table `SMRBIWMSWarehouseLocationStaging`
# ------------------------------------------------------------------
# In Databricks we use Spark SQL to create the table.  The table
# definition is translated from T‑SQL to Spark SQL types:
#   • NVARCHAR      → STRING
#   • INT           → INT
#   • BIGINT        → LONG
#   • NUMERIC(32,6) → DECIMAL(32,6)
#   • DATETIME      → TIMESTAMP
#
# Primary‑key and clustering options from T‑SQL are omitted because
# Spark/Delta Lake does not enforce them the same way.
# ------------------------------------------------------------------

# Drop the table if it already exists (avoids errors on rerun)
spark.sql(
    f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIWMSWarehouseLocationStaging`"
)

# COMMAND ----------

# Create the table.  We use a Delta location for durability but the
# storage format can be changed to `PARQUET` or `CSV` if desired.
spark.sql(
    f"""
    CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWMSWarehouseLocationStaging` (
        DEFINITIONGROUP          STRING   NOT NULL,
        EXECUTIONID              STRING   NOT NULL,
        ISSELECTED               INT      NOT NULL,
        TRANSFERSTATUS           INT      NOT NULL,
        WAREHOUSEID              STRING   NOT NULL,
        WAREHOUSELOCATIONID     STRING   NOT NULL,
        WAREHOUSELOCATIONPROFILEID STRING NOT NULL,
        COMPANY                  STRING   NOT NULL,
        WAREHOUSELOCATIONRECID   LONG     NOT NULL,
        MAXVOLUME                DECIMAL(32,6) NOT NULL,
        PARTITION                STRING   NOT NULL,
        DATAAREAID               STRING   NOT NULL,
        SYNCSTARTDATETIME        TIMESTAMP NOT NULL
    )
    USING DELTA
    """
)

# COMMAND ----------

# Optional: verify that the table was created by listing its schema
spark.sql(
    f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWMSWarehouseLocationStaging`"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
