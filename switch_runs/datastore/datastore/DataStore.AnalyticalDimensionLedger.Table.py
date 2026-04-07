# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AnalyticalDimensionLedger.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.AnalyticalDimensionLedger.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook – create table equivalent to the T‑SQL script
# ------------------------------------------------------------------
# The original T‑SQL script set session options (ANSI_NULLS, QUOTED_IDENTIFIER) and
# created a table [DataStore].[AnalyticalDimensionLedger] with all columns defined as
# BIGINT NOT NULL.  In Databricks we only need the table‑creation statement; the
# session options have no effect on Spark SQL so they are omitted.
#
# All object references are fully‑qualified using the placeholders
# `dbe_dbx_internships` and `datastore` that the caller must replace with the actual
# catalog and schema names in Unity Catalog.
#
# The table is created as a Delta Lake table (the default for Spark in
# Databricks).  The column definitions preserve the NOT NULL constraints
# from the source script.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`datastore`.`AnalyticalDimensionLedger` (
    LedgerDimensionId   BIGINT NOT NULL,
    MainAccount          BIGINT NOT NULL,
    Intercompany         BIGINT NOT NULL,
    BusinessSegment      BIGINT NOT NULL,
    EndCustomer          BIGINT NOT NULL,
    Department           BIGINT NOT NULL,
    LocalAccount         BIGINT NOT NULL,
    Location             BIGINT NOT NULL,
    Product              BIGINT NOT NULL,
    ShipmentContract     BIGINT NOT NULL,
    Vendor               BIGINT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
