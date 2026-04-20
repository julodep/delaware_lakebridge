# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIReqTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIReqTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table in the specified catalog and schema.
# ------------------------------------------------------------------
# T‑SQL types are converted to Spark SQL (Delta Lake) types as follows:
#   NVARCHAR, VARCHAR, TEXT           -> STRING
#   INT, BIGINT, SMALLINT, TINYINT   -> INT or LONG
#   NUMERIC(p,s) or DECIMAL(p,s)     -> DECIMAL(p,s)
#   DATETIME, DATE, TIME             -> TIMESTAMP
#   The PRIMARY KEY constraint is documented in the comment because
#   Delta Lake does not enforce primary keys.
# ------------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIReqTransStaging` (
  DEFINITIONGROUP          STRING NOT NULL,
  EXECUTIONID              STRING NOT NULL,
  ISSELECTED               INT NOT NULL,
  TRANSFERSTATUS           INT NOT NULL,
  COVQTY                   DECIMAL(32,6) NOT NULL,
  COMPANY                  STRING NOT NULL,
  DIRECTION                INT NOT NULL,
  ITEMID                   STRING NOT NULL,
  PLANVERSION              LONG NOT NULL,
  QTY                      DECIMAL(32,6) NOT NULL,
  REQTRANSREQID            LONG NOT NULL,
  REFID                    STRING NOT NULL,
  REFTYPE                  INT NOT NULL,
  REQDATE                   TIMESTAMP NOT NULL,
  ACTIONDATE                TIMESTAMP NOT NULL,
  COVINVENTDIMID           STRING NOT NULL,
  CUSTACCOUNTID           STRING NOT NULL,
  INVENTTRANSORIGIN         LONG NOT NULL,
  ACTIONTYPE               INT NOT NULL,
  ACTIONMARKED             INT NOT NULL,
  ACTIONDAYS               INT NOT NULL,
  FUTURESCALCULATED        INT NOT NULL,
  FUTURESDATE               TIMESTAMP NOT NULL,
  FUTURESDAYS               INT NOT NULL,
  FUTURESMARKED            INT NOT NULL,
  REQTIME                  INT NOT NULL,
  PARTITION                 STRING NOT NULL,
  DATAAREAID                STRING NOT NULL,
  SYNCSTARTDATETIME        TIMESTAMP NOT NULL,
  RECID                    LONG NOT NULL
  /* PRIMARY KEY (EXECUTIONID, REQTRANSREQID, DATAAREAID, PARTITION)
     defined in the source T‑SQL but not enforced by Delta Lake. */
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
