# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProdJournalBOMStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProdJournalBOMStaging.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------------------
#  Create the SMRBIProdJournalBOMStaging table in Databricks
#
#  Databricks uses Delta Lake / Spark SQL for table definition.
#  The T‑SQL script creates a clustered primary‑key table with a number
#  of non‑clustered columns.  Delta Lake does not support native
#  PRIMARY KEY or CLUSTERED constraints, so we simply create the table
#  with the required columns (NOT NULL) and note the missing constraint
#  in the comment.
#
#  All catalog/schema/object references are fully qualified:
#      dbe_dbx_internships.dbo.SMRBIProdJournalBOMStaging
#
#  Data‑type mapping:
#      NVARCHAR → STRING
#      INT      → INT
#      BIGINT   → LONG
#      DATETIME → TIMESTAMP
#
#  If you want to enforce uniqueness you would need to add a separate
#  constraint table or use Delta Lake 3.0+ “PRIMARY KEY” syntax in a
#  future release.  For now we only create the columns.
#
# ----------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProdJournalBOMStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    INVENTTRANSID STRING NOT NULL,
    ITEMID STRING NOT NULL,
    JOURNALID STRING NOT NULL,
    PRODID STRING NOT NULL,
    PRODJOURNALBOMRECID LONG NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
