# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjCostTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjCostTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
#  Create a persistent Delta table that mirrors the T‑SQL
#  definition of dbo.SMRBIProjCostTransStaging.
#
#  Notes on the translation:
#  • Every object reference is fully‑qualified in the form
#    `dbe_dbx_internships`.`dbo`.`<object_name>`.
#  • All SQL data types are mapped to Spark SQL types:
#      NVARCHAR  → STRING
#      NUMERIC   → DECIMAL(p,s)
#      INT       → INT
#      BIGINT    → BIGINT
#      DATETIME  → TIMESTAMP
#  • PRIMARY KEY constraints are not supported in Delta Lake;
#    they are commented out for reference only.
#  • The table is created with the DELTA format so it behaves
#    exactly like a lake‑table in Databricks.
# ------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjCostTransStaging` (
    CATEGORYID          STRING  NOT NULL,
    CURRENCYID          STRING  NOT NULL,
    LINEPROPERTYID      STRING  NOT NULL,
    PROJID              STRING  NOT NULL,
    TRANSID             STRING  NOT NULL,
    TRANSIDREF          STRING  NOT NULL,
    DEFINITIONGROUP     STRING  NOT NULL,
    EXECUTIONID         STRING  NOT NULL,
    ISSELECTED          INT     NOT NULL,
    TRANSFERSTATUS      INT     NOT NULL,
    ACTIVITYNUMBER      STRING  NOT NULL,
    QTY                 DECIMAL(32,6) NOT NULL,
    TOTALCOSTAMOUNTCUR  DECIMAL(32,6) NOT NULL,
    TOTALSALESAMOUNTCUR DECIMAL(32,6) NOT NULL,
    TRANSACTIONORIGIN   INT     NOT NULL,
    TRANSDATE           TIMESTAMP NOT NULL,
    TXT                 STRING  NOT NULL,
    COMPANY             STRING  NOT NULL,
    RESOURCE_           BIGINT  NOT NULL,
    WORKER              BIGINT  NOT NULL,
    PARTITION           STRING  NOT NULL,
    DATAAREAID           STRING  NOT NULL,
    SYNCSTARTDATETIME   TIMESTAMP NOT NULL
)
USING DELTA
"""

#  Note: Delta Lake does not support a native PRIMARY KEY.
#  The following PRIMARY KEY clause that existed in T‑SQL
#  is retained here as a comment for reference.
#  COMMENT 'PRIMARY KEY (TRANSID, EXECUTIONID, DATAAREAID, PARTITION)' 
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
