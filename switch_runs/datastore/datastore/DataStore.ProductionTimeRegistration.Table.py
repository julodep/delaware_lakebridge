# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductionTimeRegistration.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ProductionTimeRegistration.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the ProductionTimeRegistration table in the specified
# Unity Catalog catalog and schema.
#
# The column definitions are mapped from the original T‑SQL
# types to Spark SQL data types as per the guidelines:
#   NVARCHAR -> STRING
#   INT      -> INT
#   BIGINT   -> LONG
#   NUMERIC  -> DECIMAL(p, s)
#   DATE     -> DATE
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`datastore`.`ProductionTimeRegistration` (
    ProductionOrderCode          STRING  NOT NULL,
    CompanyCode                  STRING  NOT NULL,
    ProductConfigurationCode     STRING  NOT NULL,
    RouteCode                     STRING  NOT NULL,
    RoutingName                   STRING  NOT NULL,
    ResourceCode                  STRING  NOT NULL,
    OperationCode                 STRING  NOT NULL,
    OperationNumber                INT  NOT NULL,
    Shift                         STRING  NOT NULL,
    OperatorType                  STRING  NOT NULL,
    OperatorName                  LONG  NOT NULL,
    RecId                          LONG  NOT NULL,
    PostedJournalDate              DATE,
    Hours                         DECIMAL(32, 6) NOT NULL,
    HourPrice                      DECIMAL(32, 6) NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
