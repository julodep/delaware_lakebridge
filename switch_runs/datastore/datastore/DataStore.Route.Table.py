# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Route.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Route.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook – create the Route table in dbe_dbx_internships.datastore
#
# All identifiers are fully‑qualified with the target catalog and schema.
# NVARCHAR / NVARCHAR(max) in T‑SQL are mapped to STRING in Spark SQL.
# NULL/NOT NULL constraints are preserved where supported.
# ------------------------------------------------------------------

# Create or replace the Route table with the same schema as the original T‑SQL
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Route` (
    RouteCode            STRING NOT NULL,
    RouteName            STRING NOT NULL,
    RouteCodeName        STRING,
    OperationCode        STRING NOT NULL,
    OperationSequence    BIGINT,
    OperationNumber      INT NOT NULL,
    OperationNumberNext  INT NOT NULL,
    CompanyCode          STRING NOT NULL,
    RouteGroupCode       STRING NOT NULL,
    RouteGroupName       STRING NOT NULL,
    RouteGroupCodeName   STRING,
    SiteCode             STRING NOT NULL,
    SiteName             STRING NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
