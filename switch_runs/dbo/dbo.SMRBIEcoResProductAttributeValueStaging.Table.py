# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIEcoResProductAttributeValueStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIEcoResProductAttributeValueStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# Define the staging table in Delta Lake
# --------------------------------------------------------------------
#
# The original T‑SQL object is a clustered primary‑key table that lives
# in the `dbo` schema.  In Databricks we recreate an equivalent table
# using the fully‑qualified name: `dbe_dbx_internships`.`dbo`.SMRBIEcoResProductAttributeValueStaging.
#
# Data‑type mappings:
#   - NVARCHAR / VARCHAR → STRING
#   - INT                → INT
#   - BIGINT             → BIGINT
#   - NUMERIC(32,6)      → DECIMAL(32,6)
#   - DATETIME           → TIMESTAMP
#
# Delta Lake does not yet support primary‑key constraints on a
# per‑row level, so we document the intended key but leave the actual
# enforcement to downstream processes or external tools.
#
# --------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIEcoResProductAttributeValueStaging
(
    DEFINITIONGROUP        STRING  NOT NULL,
    EXECUTIONID            STRING  NOT NULL,
    ISSELECTED             INT     NOT NULL,
    TRANSFERSTATUS         INT     NOT NULL,
    ATTRIBUTENAME          STRING  NOT NULL,
    ATTRIBUTETYPENAME      STRING  NOT NULL,
    CURRENCYCODE           STRING  NOT NULL,
    BOOLEANVALUE           INT     NOT NULL,
    DATATYPE               INT     NOT NULL,
    DATETIMEVALUE          TIMESTAMP NOT NULL,
    DECIMALVALUE           DECIMAL(32,6) NOT NULL,
    INTEGERVALUE           INT     NOT NULL,
    NAME                   STRING  NOT NULL,
    PRODUCTNUMBER          STRING  NOT NULL,
    TEXTVALUE              STRING  NOT NULL,
    PARTITION              STRING  NOT NULL,
    SYNCSTARTDATETIME      TIMESTAMP NOT NULL,
    RECID                  BIGINT  NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
