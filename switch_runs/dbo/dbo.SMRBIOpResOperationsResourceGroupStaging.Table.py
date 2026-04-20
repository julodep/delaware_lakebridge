# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIOpResOperationsResourceGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIOpResOperationsResourceGroupStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Databricks notebook – Create the staging table
# --------------------------------------------------------------
# This snippet converts the T‑SQL 'CREATE TABLE' for
# dbo.SMRBIOpResOperationsResourceGroupStaging into a Databricks
# (Delta Lake) table definition.
#
# 1.  All T‑SQL data types are mapped to Spark SQL types:
#         • NVARCHAR   → STRING
#         • INT        → INT
#         • BIGINT     → LONG
#         • DATETIME   → TIMESTAMP
# 2.  Primary‑key constraints are not supported in Delta Lake
#     (until ACID compliance with Spark SQL 3.x).  The PK
#     definition has therefore been commented out for reference.
# --------------------------------------------------------------

# Replace the placeholder values with your actual catalog and schema names.
# For example: catalog = "my_catalog", schema = "my_schema"
catalog = "dbe_dbx_internships"
schema  = "dbo"

# COMMAND ----------

# Create (or replace) the staging table with the matching schema.
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIOpResOperationsResourceGroupStaging` (
    DEFINITIONGROUP          STRING  NOT NULL,
    EXECUTIONID              STRING  NOT NULL,
    ISSELECTED               INT     NOT NULL,
    TRANSFERSTATUS           INT     NOT NULL,
    INPUTWAREHOUSEID         STRING  NOT NULL,
    INPUTWAREHOUSELOCATIONID STRING  NOT NULL,
    OUTPUTWAREHOUSEID        STRING  NOT NULL,
    OUTPUTWAREHOUSELOCATIONID STRING  NOT NULL,
    PRODUCTIONUNITID         STRING  NOT NULL,
    GROUPID                  STRING  NOT NULL,
    GROUPNAME                STRING  NOT NULL,
    COMPANY                  STRING  NOT NULL,
    OPRESOPERATIONSRESOURCEGROUPRECID LONG  NOT NULL,
    PARTITION                STRING  NOT NULL,
    DATAAREAID               STRING  NOT NULL,
    SYNCSTARTDATETIME        TIMESTAMP NOT NULL,
    RECID                    LONG    NOT NULL
    /* 
     * Primary key definition does not map directly to Delta Lake.
     * If you require a 1‑to‑1 uniqueness guarantee,
     * consider adding a unique index or using Delta Lake's
     * optimistic concurrency controls.
     *
     * PRIMARY KEY (EXECUTIONID, GROUPID, DATAAREAID, PARTITION)
     */
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
