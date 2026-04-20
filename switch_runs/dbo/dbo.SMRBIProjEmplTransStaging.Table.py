# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjEmplTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjEmplTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the `SMRBIProjEmplTransStaging` table in Databricks
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjEmplTransStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ACTIVITYNUMBER STRING NOT NULL,
    CATEGORYID STRING NOT NULL,
    CURRENCYID STRING NOT NULL,
    LINEPROPERTYID STRING NOT NULL,
    PROJID STRING NOT NULL,
    QTY DECIMAL(32,6) NOT NULL,
    TOTALCOSTAMOUNTCUR DECIMAL(32,6) NOT NULL,
    TOTALSALESAMOUNTCUR DECIMAL(32,6) NOT NULL,
    TRANSDATE TIMESTAMP NOT NULL,
    TRANSID STRING NOT NULL,
    TXT STRING NOT NULL,
    COMPANY STRING NOT NULL,
    WORKER BIGINT NOT NULL,
    `PARTITION` STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
