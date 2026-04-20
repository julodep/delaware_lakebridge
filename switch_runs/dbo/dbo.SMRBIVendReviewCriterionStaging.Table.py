# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendReviewCriterionStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendReviewCriterionStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the staging table in Unity Catalog
# ------------------------------------------------------------------
#    - All identifiers must be fully‑qualified:
#        dbe_dbx_internships.dbo.SMRBIVendReviewCriterionStaging
#    - Delta Lake (Databricks) does not support primary‑key constraints nor
#      statistics options (STATISTICS_NORECOMPUTE, etc.).  
#    - We create the table with the same column types and then add a comment
#      to document the intended PK for downstream users.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIVendReviewCriterionStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT    NOT NULL,
    TRANSFERSTATUS INT   NOT NULL,
    CRITERIONGROUP LONG   NOT NULL,
    NAME          STRING NOT NULL,
    VENDREVIEWCRITERIONRECID LONG NOT NULL,
    PARTITION     STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID         LONG   NOT NULL
)
USING DELTA
COMMENT 'Intended PK: (EXECUTIONID, CRITERIONGROUP, NAME, PARTITION)'
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Verify the table was created with the right schema
# ------------------------------------------------------------------
df = spark.table(f"dbe_dbx_internships.dbo.SMRBIVendReviewCriterionStaging")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
