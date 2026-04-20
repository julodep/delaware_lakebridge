# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWrkCtrRouteOprActivityStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWrkCtrRouteOprActivityStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------------
# Databricks notebook – Create the table SMRBIWrkCtrRouteOprActivityStaging
# ---------------------------------------------------------------
# The original T‑SQL creates a permanent table with a primary key.
# In Snowflake‑style or Delta Lake the primary‑key constraint is not enforced,
# so we simply define the columns.  Add a comment to the table properties
# if you need to track PK metadata for documentation purposes.
# ---------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWrkCtrRouteOprActivityStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ACTIVITY       BIGINT NOT NULL,
    ROUTEOPR       BIGINT NOT NULL,
    ROUTEOPRDATAAREAID STRING NOT NULL,
    PARTITION      STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID          BIGINT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# ---------------------------------------------------------------
# Optional: Add a comment on the table to indicate the original PK.
# ---------------------------------------------------------------
spark.sql(f"""
ALTER TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWrkCtrRouteOprActivityStaging`
SET TBLPROPERTIES (
  'comment' = 'Original T‑SQL PRIMARY KEY (EXECUTIONID, ACTIVITY, ROUTEOPR, ROUTEOPRDATAAREAID, PARTITION)'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
