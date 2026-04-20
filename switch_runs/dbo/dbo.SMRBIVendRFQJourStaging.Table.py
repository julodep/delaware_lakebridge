# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendRFQJourStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendRFQJourStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the SMRBIVendRFQJourStaging table in Databricks (Delta‑Lake)
# ------------------------------------------------------------------
# Primary‑key enforcement is not supported in Spark/Databricks, so we
# simply create the table with the column definitions and add a comment
# that lists the intended PK columns.
# ------------------------------------------------------------------

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIVendRFQJourStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    INTERNALRFQID STRING NOT NULL,
    RFQCASEID STRING NOT NULL,
    RFQDATE TIMESTAMP NOT NULL,
    RFQID STRING NOT NULL,
    VENDACCOUNT STRING NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
USING DELTA
COMMENT 'Primary key: EXECUTIONID, INTERNALRFQID, RFQDATE, RFQID, DATAAREAID, PARTITION'
;
"""

# COMMAND ----------

spark.sql(create_table_sql)

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Verify creation by showing the table schema
# ------------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendRFQJourStaging`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
