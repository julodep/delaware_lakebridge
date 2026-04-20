# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIsmmBusinessSubsegmentStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIsmmBusinessSubsegmentStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table in the specified Delta‑Lake catalog.
# Replace `dbe_dbx_internships` and `dbo` with your actual names before running.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIsmmBusinessSubsegmentStaging` (
    DEFINITIONGROUP        STRING NOT NULL,
    EXECUTIONID            STRING NOT NULL,
    ISSELECTED             INT NOT NULL,
    TRANSFERSTATUS         INT NOT NULL,
    BUSINESSSEGMENTCODE    STRING NOT NULL,
    SUBSEGMENTDESCRIPTION  STRING NOT NULL,
    SUBSEGMENTCODE         STRING NOT NULL,
    PARTITION              STRING NOT NULL,
    DATAAREAID             STRING NOT NULL,
    SYNCSTARTDATETIME      TIMESTAMP NOT NULL
)
USING delta
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Verify that the table has been created correctly.
# ------------------------------------------------------------------
spark.sql(f"DESC TABLE `dbe_dbx_internships`.`dbo`.`SMRBIsmmBusinessSubsegmentStaging`").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
