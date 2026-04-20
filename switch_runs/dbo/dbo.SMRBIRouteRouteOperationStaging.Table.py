# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIRouteRouteOperationStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIRouteRouteOperationStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table in the target catalog & schema
# ------------------------------------------------------------------
# Because Delta Lake (and Databricks SQL) does not support INSTANT primary‑key
# constraints, we comment the original PK definition and rely on
# application logic or downstream checks if uniqueness must be enforced.
# ------------------------------------------------------------------

# Drop the table if it already exists (for idempotent notebook runs)
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIRouteRouteOperationStaging`")

# COMMAND ----------

# Create the table using the DELTA engine.  The column definitions map directly
# from the T‑SQL data types (NVARCHAR → STRING, INT → INT, BIGINT → LONG,
# DATETIME → TIMESTAMP).  All columns are declared NOT NULL to match the
# original T‑SQL schema.  The original PRIMARY KEY constraint is omitted
# because Delta cannot enforce it automatically.
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIRouteRouteOperationStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    NEXTROUTEOPERATIONNUMBER INT NOT NULL,
    OPERATIONID STRING NOT NULL,
    OPERATIONNUMBER INT NOT NULL,
    OPERATIONPRIORITY INT NOT NULL,
    ROUTEID STRING NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Add a comment or tag indicating the original PK for future reference
# ------------------------------------------------------------------
spark.sql(f"""
COMMENT ON TABLE `dbe_dbx_internships`.`dbo`.`SMRBIRouteRouteOperationStaging` 
IS 'Original primary key: EXECUTIONID, OPERATIONNUMBER, OPERATIONPRIORITY, ROUTEID, DATAAREAID, PARTITION'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
