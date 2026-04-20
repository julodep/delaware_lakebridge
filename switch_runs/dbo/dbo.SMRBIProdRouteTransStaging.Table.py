# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProdRouteTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProdRouteTransStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Databricks notebook – Staging table creation
# -------------------------------------------------------------------------
# This script creates the table `SMRBIProdRouteTransStaging` in the
# specified catalog and schema. All names are fully‑qualified:
# `dbe_dbx_internships.dbo.SMRBIProdRouteTransStaging`
#
# Notes on translation:
#  • T‑SQL data types were mapped to Spark SQL types using the
#    guidelines in the prompt (NVARCHAR → STRING, DATETIME → TIMESTAMP,
#    NUMERIC(p,s) → DECIMAL(p,s), BIGINT → LONG, INT → INT, etc.).
#  • The T‑SQL primary‑key declaration is not enforced by Delta Lake
#    by default, so it is omitted in the DDL. A comment is left in
#    the script to remind the user that the key constraint is not active.
#  • Square‑bracket identifiers (e.g. `[dbo]`) are removed because
#    Spark SQL does not support them.  The schema name is supplied
#    externally via the `dbo` placeholder.
# -------------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProdRouteTransStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    AMOUNT DECIMAL(32,6) NOT NULL,
    CATEGORYID STRING NOT NULL,
    COMPANY STRING NOT NULL,
    DATEWIP TIMESTAMP NOT NULL,
    HOURPRICE DECIMAL(32,6) NOT NULL,
    HOURS DECIMAL(32,6) NOT NULL,
    OPRID STRING NOT NULL,
    OPRNUM INT NOT NULL,
    QTYERROR DECIMAL(32,6) NOT NULL,
    QTYGOOD DECIMAL(32,6) NOT NULL,
    PRODROUTETRANSRECID BIGINT NOT NULL,
    TRANSREFID STRING NOT NULL,
    TRANSREFTYPE INT NOT NULL,
    TRANSTYPE INT NOT NULL,
    WORKER BIGINT NOT NULL,
    WRKCTRID STRING NOT NULL,
    `PARTITION` STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
COMMENT='Table SMRBIProdRouteTransStaging';
""")

# COMMAND ----------

# Verify that the table was created
spark.sql(f"SHOW TABLES IN `dbe_dbx_internships`.`dbo`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near '='. SQLSTATE: 42601 (line 1, pos 841)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBIProdRouteTransStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     AMOUNT DECIMAL(32,6) NOT NULL,     CATEGORYID STRING NOT NULL,     COMPANY STRING NOT NULL,     DATEWIP TIMESTAMP NOT NULL,     HOURPRICE DECIMAL(32,6) NOT NULL,     HOURS DECIMAL(32,6) NOT NULL,     OPRID STRING NOT NULL,     OPRNUM INT NOT NULL,     QTYERROR DECIMAL(32,6) NOT NULL,     QTYGOOD DECIMAL(32,6) NOT NULL,     PRODROUTETRANSRECID BIGINT NOT NULL,     TRANSREFID STRING NOT NULL,     TRANSREFTYPE INT NOT NULL,     TRANSTYPE INT NOT NULL,     WORKER BIGINT NOT NULL,     WRKCTRID STRING NOT NULL,     `PARTITION` STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID BIGINT NOT NULL ) COMMENT='Table SMRBIProdRouteTransStaging';
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
