# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPurchRFQCaseLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPurchRFQCaseLineStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------- #
#  Create a persistent Delta table in Unity Catalog
#  Fully-qualified name:  `dbe_dbx_internships`.`dbo`.SMRBIPurchRFQCaseLineStaging
#
#  All column definitions follow the T‑SQL schema but are mapped to Spark SQL
#  data types:
#      NVARCHAR(n) → STRING
#      INT          → INT
#      BIGINT       → BIGINT
#      DATETIME     → TIMESTAMP
#  Constraints such as PRIMARY KEY, CLUSTERED, STATISTICS etc. are not
#  supported natively in Delta Lake; they are noted as comments.
# --------------------------------------------------------------------------- #

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.SMRBIPurchRFQCaseLineStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT    NOT NULL,
    TRANSFERSTATUS INT   NOT NULL,
    COMPANY        STRING NOT NULL,
    ITEMID         STRING NOT NULL,
    LINENUMBER     BIGINT NOT NULL,
    RFQCASEID      STRING NOT NULL,
    STATUSHIGH     INT    NOT NULL,
    STATUSLOW      INT    NOT NULL,
    PARTITION      STRING NOT NULL,
    DATAAREAID     STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
