# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventTestOperationMiscChargesStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventTestOperationMiscChargesStaging.Table.sql`

# COMMAND ----------

# -------------------------------
#   Create table SMRBIInventTestOperationMiscChargesStaging
# -------------------------------
#   • All identifiers are fully‑qualified: `dbe_dbx_internships`.`dbo`.ObjectName
#   • T‑SQL data types have been mapped to Spark SQL types:
#       NVARCHAR(n)  → STRING
#       NUMERIC(p,s) → DECIMAL(p,s)
#       DATETIME     → TIMESTAMP
#   • Index and statistics options that exist only in T‑SQL are omitted –
#     Delta Lake (Databricks) does not support those clauses.
#   • PRIMARY KEY is defined in a standard way that Delta Lake accepts.
#   • Temporary or staging tables usually remain in the catalog; if you
#     want a true temp table you can use `CREATE TABLE IF NOT EXISTS` and
#     drop it manually later.
# -------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIInventTestOperationMiscChargesStaging` (
    DEFINITIONGROUP          STRING      NOT NULL,
    EXECUTIONID              STRING      NOT NULL,
    ISSELECTED               INT         NOT NULL,
    TRANSFERSTATUS           INT         NOT NULL,
    INVENTNONCONFORMANCEID   STRING      NOT NULL,
    LINENUM                   DECIMAL(32,16) NOT NULL,
    MARKUPCODE                STRING      NOT NULL,
    TXT                       STRING      NOT NULL,
    VALUE                     DECIMAL(32,6)  NOT NULL,
    COMPANY                   STRING      NOT NULL,
    PARTITION                 STRING      NOT NULL,
    DATAAREAID                STRING      NOT NULL,
    SYNCSTARTDATETIME         TIMESTAMP   NOT NULL,
    RECID                     BIGINT      NOT NULL,
    PRIMARY KEY (EXECUTIONID, INVENTNONCONFORMANCEID, LINENUM,
                MARKUPCODE, TXT, VALUE, DATAAREAID, PARTITION)
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
