# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjectItemJournalTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjectItemJournalTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table in the target Delta Lake catalog & schema
# ------------------------------------------------------------------
# The original T‑SQL uses a temporary object with a primary key.
# Delta Lake does not enforce primary keys, so we simply create the
# table and add a comment noting the original PK definition.
# All object names are fully‑qualified using the dbe_dbx_internships.dbo placeholders.
# ------------------------------------------------------------------

spark.sql(f"""
    CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjectItemJournalTableStaging` (
        DEFINITIONGROUP STRING,
        EXECUTIONID STRING,
        ISSELECTED INT,
        TRANSFERSTATUS INT,
        DESCRIPTION STRING,
        JOURNALID STRING,
        JOURNALNAME STRING,
        COMPANY STRING,
        PARTITION STRING,
        DATAAREAID STRING,
        SYNCSTARTDATETIME TIMESTAMP
    )
    USING delta
""")

# COMMAND ----------

# Add a table comment to preserve metadata and note the original PK
spark.sql(f"""
    COMMENT ON TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjectItemJournalTableStaging`
    IS 'Staging table for SMRBI Project Item Journals. Original PK: (EXECUTIONID, JOURNALID, DATAAREAID, PARTITION)'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
