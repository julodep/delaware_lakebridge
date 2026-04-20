# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendSettlementStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendSettlementStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Databricks (Spark SQL) table creation – fully‑qualified name
#  The table name is derived from the original `dbo.SMRBIVendSettlementStaging`.
#  All object references include the catalog and schema placeholders
#  `dbe_dbx_internships` and `dbo` which you can replace with your actual
#  Unity Catalog identifiers.
#
#  Data‑type mapping:
#     NVARCHAR           → STRING
#     INT                → INT
#     BIGINT             → BIGINT
#     NUMERIC(p, s)      → DECIMAL(p, s)
#     DATETIME           → TIMESTAMP
# ------------------------------------------------------------------
spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendSettlementStaging` (
        DEFINITIONGROUP          STRING      NOT NULL,
        EXECUTIONID              STRING      NOT NULL,
        ISSELECTED                INT      NOT NULL,
        TRANSFERSTATUS            INT      NOT NULL,
        ACCOUNTNUM               STRING      NOT NULL,
        EXCHADJUSTMENT            DECIMAL(32,6) NOT NULL,
        SETTLEAMOUNTCUR            DECIMAL(32,6) NOT NULL,
        SETTLEAMOUNTMST            DECIMAL(32,6) NOT NULL,
        TRANSCOMPANY              STRING      NOT NULL,
        TRANSDATE                 TIMESTAMP   NOT NULL,
        TRANSRECID                 BIGINT     NOT NULL,
        VENDSETTLEMENTRECID       BIGINT     NOT NULL,
        PARTITION                 STRING      NOT NULL,
        DATAAREAID                STRING      NOT NULL,
        SYNCSTARTDATETIME        TIMESTAMP   NOT NULL,
        RECID                      BIGINT     NOT NULL
    );
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
