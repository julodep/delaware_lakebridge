# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPurchRFQCaseTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPurchRFQCaseTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook – create a staging table in Unity Catalog
# Fully‑qualified names use the placeholders dbe_dbx_internships and dbo.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIPurchRFQCaseTableStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    COMPANY STRING NOT NULL,
    NAME STRING NOT NULL,
    RFQCASEID STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING delta
""")

# COMMAND ----------

# NOTE: The original T‑SQL contained a primary key constraint.  
# Delta Lake does not support PK or clustering in the CREATE TABLE
# statement.  If a unique constraint is needed, add one after
# populating the table or use a Merkle hash column.

# ------------------------------------------------------------------
# Optional: show the schema of the newly created table
# ------------------------------------------------------------------
spark.table(f"dbe_dbx_internships.dbo.SMRBIPurchRFQCaseTableStaging").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
