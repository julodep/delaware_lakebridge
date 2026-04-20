# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICaseAssociationStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICaseAssociationStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# This notebook creates the staging table `SMRBICaseAssociationStaging`
# inside the target catalog and schema.  
# 
# T‑SQL specifics:
#   * Square‑bracket notation (e.g. [dbo].[SMRBICaseAssociationStaging])
#     is removed – Databricks uses dot notation.
#   * The primary‑key constraint is not supported in Delta Lake/Databricks
#     tables.  We create the table without any constraint and leave a
#     comment that the PK has been ignored so users can apply index
#     logic manually if needed.
#   * Column data types are mapped to Spark SQL types:
#       nvarchar → STRING
#       int      → INT
#       bigint   → BIGINT
#       datetime → TIMESTAMP
# ------------------------------------------------------------------

catalog = "dbe_dbx_internships"
schema  = "dbo"
table   = "SMRBICaseAssociationStaging"

# COMMAND ----------

# Create the table as a Delta Lake table in the metadata catalog.
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`{table}`
(
    DEFINITIONGROUP   STRING NOT NULL,
    EXECUTIONID       STRING NOT NULL,
    ISSELECTED        INT    NOT NULL,
    TRANSFERSTATUS    INT    NOT NULL,
    CASERECID         BIGINT NOT NULL,
    ENTITYTYPE        INT    NOT NULL,
    ISPRIMARY         INT    NOT NULL,
    REFRECID          BIGINT NOT NULL,
    PARTITION         STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID             BIGINT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
