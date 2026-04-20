# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventSettlementStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventSettlementStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
#  T‑SQL CREATE TABLE → Databricks Delta Table
#  =========================================
#  This snippet recreates the T‑SQL table
#  dbo.SMRBIInventSettlementStaging
#  in the catalog `dbe_dbx_internships` and schema `dbo`.
# -------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventSettlementStaging` (
    DEFINITIONGROUP   STRING  NOT NULL,
    EXECUTIONID       STRING  NOT NULL,
    ISSELECTED        INT     NOT NULL,
    TRANSFERSTATUS    INT     NOT NULL,
    COMPANY           STRING  NOT NULL,
    QTYSETTLED        DECIMAL(32,16) NOT NULL,
    INVENTSETTLEMENTRECID BIGINT NOT NULL,
    SETTLETRANSID     STRING  NOT NULL,
    TRANSRECID        BIGINT NOT NULL,
    PARTITION         STRING  NOT NULL,
    DATAAREAID        STRING  NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
    -- PRIMARY KEY (EXECUTIONID, INVENTSETTLEMENTRECID, DATAAREAID, PARTITION)
)
""")

# COMMAND ----------

# -------------------------------------------------------------
# Verify table creation by showing its schema
# -------------------------------------------------------------
display(spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIInventSettlementStaging`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 667)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIInventSettlementStaging` (     DEFINITIONGROUP   STRING  NOT NULL,     EXECUTIONID       STRING  NOT NULL,     ISSELECTED        INT     NOT NULL,     TRANSFERSTATUS    INT     NOT NULL,     COMPANY           STRING  NOT NULL,     QTYSETTLED        DECIMAL(32,16) NOT NULL,     INVENTSETTLEMENTRECID BIGINT NOT NULL,     SETTLETRANSID     STRING  NOT NULL,     TRANSRECID        BIGINT NOT NULL,     PARTITION         STRING  NOT NULL,     DATAAREAID        STRING  NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL     -- PRIMARY KEY (EXECUTIONID, INVENTSETTLEMENTRECID, DATAAREAID, PARTITION) )
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
