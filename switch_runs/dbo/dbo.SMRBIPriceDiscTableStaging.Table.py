# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPriceDiscTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPriceDiscTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table SMRBIPriceDiscTableStaging
# ------------------------------------------------------------------
# NOTE:
#   • All references are fully‑qualified: `dbe_dbx_internships`.`dbo`.`SMRBIPriceDiscTableStaging`
#   • Square‑bracket identifiers and GO statements are removed.
#   • Data types are mapped from T‑SQL to Spark/Delta types:
#     NVARCHAR → STRING, INT → INT, BIGINT → LONG,
#     NUMERIC(p,s) → DECIMAL(p,s), DATETIME → TIMESTAMP
#   • A Delta primary‑key constraint is added.  Delta enforces this
#     only if the cluster property `spark.databricks.delta.constraints.enabled`
#     is set to true (this can be configured in the cluster config).
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIPriceDiscTableStaging` (
    DEFINITIONGROUP        STRING     NOT NULL,
    EXECUTIONID            STRING     NOT NULL,
    ISSELECTED             INT        NOT NULL,
    TRANSFERSTATUS         INT        NOT NULL,
    ACCOUNTRELATION       STRING     NOT NULL,
    AMOUNT                 DECIMAL(32,6) NOT NULL,
    CURRENCY               STRING     NOT NULL,
    COMPANY                STRING     NOT NULL,
    FROMDATE               TIMESTAMP  NOT NULL,
    ITEMRELATION           STRING     NOT NULL,
    PRICEDISCTABLEMODIFIEDDATETIME TIMESTAMP  NOT NULL,
    ORIGINALPRICEDISCADMTRANSRECID      LONG   NOT NULL,
    PRICEDISCTABLEPARTITION              LONG   NOT NULL,
    PDSCALCULATIONID      STRING     NOT NULL,
    PRICEUNIT              DECIMAL(32,12) NOT NULL,
    QUANTITYAMOUNTFROM     DECIMAL(32,6) NOT NULL,
    QUANTITYAMOUNTTO       DECIMAL(32,6) NOT NULL,
    PRICEDISCTABLERECID    LONG     NOT NULL,
    TODATE                 TIMESTAMP  NOT NULL,
    UNITID                 STRING     NOT NULL,
    MODULE                 INT        NOT NULL,
    CONFIGID               STRING     NOT NULL,
    INVENTCOLORID          STRING     NOT NULL,
    INVENTSIZEID           STRING     NOT NULL,
    PARTITION              STRING     NOT NULL,
    DATAAREAID             STRING     NOT NULL,
    SYNCSTARTDATETIME      TIMESTAMP  NOT NULL,
    RECID                  LONG       NOT NULL,
    CONSTRAINT PK_SMRBIPriceDiscTableStaging
        PRIMARY KEY (EXECUTIONID, PRICEDISCTABLERECID, DATAAREAID, PARTITION)
) USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Verify table creation (display one row)
# ------------------------------------------------------------------
spark.table(f"`dbe_dbx_internships`.`dbo`.`SMRBIPriceDiscTableStaging`").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
