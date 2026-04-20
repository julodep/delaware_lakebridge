# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIWrkCtrActivityResourceRequirementStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIWrkCtrActivityResourceRequirementStaging.Table.sql`

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 1️⃣  Create the staging table in the target Unity Catalog namespace.
#
#     •  All object references are fully‑qualified: dbe_dbx_internships.dbo.SMRBIWrkCtrActivityResourceRequirementStaging
#     •  Column data types are mapped from T‑SQL to Spark SQL data types:
#          •  NVARCHAR  → STRING
#          •  INT       → INT
#          •  BIGINT    → LONG
#          •  DATETIME  → TIMESTAMP
#     •  Primary key definition, clustering, and index hinting are supported in T‑SQL but are not natively
#        supported in Delta Lake.  We therefore add the column definitions and leave a commented‑out
#        section explaining the original T‑SQL constraint – this keeps the DDL readable for future
#        reference without breaking the schema.
# ──────────────────────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIWrkCtrActivityResourceRequirementStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID    STRING,
    ISSELECTED     INT,
    TRANSFERSTATUS INT,
    ACTIVITYREQUIREMENT LONG,
    RESOURCEDATAAREAID STRING,
    WRKCTRID       STRING,
    PARTITION      STRING,
    SYNCSTARTDATETIME TIMESTAMP,
    RECID          LONG
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
