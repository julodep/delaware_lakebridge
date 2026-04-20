# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDirPartyBaseStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDirPartyBaseStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣ Create a permanent Delta table with the same schema as the
#    original T‑SQL statement.
# ------------------------------------------------------------------
#    - NVARCHAR → STRING
#    - INT      → INT        (Spark SQL INT)
#    - BIGINT   → LONG       (Spark SQL LONG)
#    - DATETIME → TIMESTAMP
#    - PRIMARY KEY is added for metadata; Delta Lake does NOT enforce
#      primary‑key constraints at write time.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIDirPartyBaseStaging (
    DEFINITIONGROUP          STRING  NOT NULL,
    EXECUTIONID              STRING  NOT NULL,
    ISSELECTED               INT     NOT NULL,
    TRANSFERSTATUS           INT     NOT NULL,
    PARTYNUMBER              STRING  NOT NULL,
    NAME                     STRING  NOT NULL,
    INSTANCERELATIONTYPE     LONG    NOT NULL,
    PARTITION                STRING  NOT NULL,
    SYNCSTARTDATETIME        TIMESTAMP NOT NULL,
    RECID                    LONG    NOT NULL,
    PRIMARY KEY (EXECUTIONID, PARTYNUMBER, PARTITION)
)
COMMENT 'Metadata only';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
