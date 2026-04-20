# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProdJournalTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProdJournalTableStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
#  Create a staging table `SMRBIProdJournalTableStaging`
# --------------------------------------------------------------------
# 1.   Translate the T‑SQL column list:
#      - NVARCHAR(n)   -> STRING
#      - INT           -> INT
#      - DATETIME      -> TIMESTAMP
# 2.   Spark/Delta Lake does **not** support PRIMARY KEY or CLUSTERED
#      constraints in the CREATE TABLE statement.  Those are omitted
#      and a comment is left in the code explaining the omission.
# 3.   Object references are fully‑qualified: `dbe_dbx_internships`.`dbo`.`SMRBIProdJournalTableStaging`.
# 4   The table is created as a Delta table (recommended in Databricks).
# --------------------------------------------------------------------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProdJournalTableStaging`
    (
        DEFINITIONGROUP   STRING NOT NULL,
        EXECUTIONID       STRING NOT NULL,
        ISSELECTED        INT    NOT NULL,
        TRANSFERSTATUS    INT    NOT NULL,
        JOURNALID         STRING NOT NULL,
        PRODID            STRING NOT NULL,
        PARTITION         STRING NOT NULL,
        DATAAREAID        STRING NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
