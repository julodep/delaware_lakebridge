# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjOnAccTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjOnAccTransStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------------
# Create the staging table `SMRBIProjOnAccTransStaging` in the target catalog and
# schema. All identifiers are fully‑qualified so the notebook can be run anywhere.
# -------------------------------------------------------------------------------

spark.sql(f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjOnAccTransStaging` (
        DEFINITIONGROUP STRING NOT NULL,
        EXECUTIONID STRING NOT NULL,
        ISSELECTED INT NOT NULL,
        TRANSFERSTATUS INT NOT NULL,
        TRANSID STRING NOT NULL,
        CURRENCYID STRING NOT NULL,
        COMPANY STRING NOT NULL,
        DESCRIPTION STRING NOT NULL,
        PROJID STRING NOT NULL,
        QTY DECIMAL(32,6) NOT NULL,
        TOTALSALESAMOUNTCUR DECIMAL(32,6) NOT NULL,
        TRANSDATE TIMESTAMP NOT NULL,
        `PARTITION` STRING NOT NULL,
        DATAAREAID STRING NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL
    )
""")

# COMMAND ----------

# If you still want explicit metadata about the key you could add a comment:
spark.sql(f"""
    COMMENT ON TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjOnAccTransStaging`
        IS 'Clustered PK on EXECUTIONID, TRANSID, DATAAREAID, PARTITION';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
