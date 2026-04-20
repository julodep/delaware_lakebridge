# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendReviewCategoryCriterionGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendReviewCategoryCriterionGroupStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# NOTE: This script creates the staging table `SMRBIVendReviewCategoryCriterionGroupStaging`
# under the fully‑qualified name `dbe_dbx_internships`.`dbo`.  
#
# T‑SQL used a PRIMARY KEY and various options that Delta Lake (the default
# storage format in Databricks) does not support.  Those definitions are
# omitted and a comment is added to explain why.
#
# Data‑type mapping (T‑SQL → Spark SQL):
#   NVARCHAR → STRING
#   INT      → INT
#   BIGINT   → LONG
#   DATETIME → TIMESTAMP
#
# ------------------------------------------------------------------

# Drop the table if it already exists to allow idempotent runs.
# (Databricks Delta supports DROP TABLE IF EXISTS.)
spark.sql("DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.SMRBIVendReviewCategoryCriterionGroupStaging")

# COMMAND ----------

# Create the table with the required columns.
spark.sql("""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIVendReviewCategoryCriterionGroupStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CRITERIONGROUP LONG NOT NULL,
    PROCUREMENTCATEGORY LONG NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
