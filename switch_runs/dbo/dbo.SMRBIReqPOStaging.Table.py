# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIReqPOStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIReqPOStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
#  Databricks notebook (Python) – create table SMRBIReqPOStaging
# --------------------------------------------------------------
# NOTE: Replace `catalog` and `schema` with the actual names in your workspace.

# ------------------------------------------------------------------
# 1. Create the table with the appropriate column types
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIReqPOStaging` (
    DEFINITIONGROUP   STRING  NOT NULL,
    EXECUTIONID       STRING  NOT NULL,
    ISSELECTED        INT     NOT NULL,
    TRANSFERSTATUS    INT     NOT NULL,
    COVINVENTDIMID    STRING  NOT NULL,
    COMPANY           STRING  NOT NULL,
    ITEMBUYERGROUPID  STRING  NOT NULL,
    ITEMGROUPID       STRING  NOT NULL,
    ITEMID            STRING  NOT NULL,
    LEADTIME          INT     NOT NULL,
    PLANVERSION       LONG    NOT NULL,
    PURCHID           STRING  NOT NULL,
    PURCHQTY          DECIMAL(32,6) NOT NULL,
    PURCHUNIT         STRING  NOT NULL,
    QTY               DECIMAL(32,6) NOT NULL,
    REFID             STRING  NOT NULL,
    REFTYPE           INT     NOT NULL,
    REQDATE           TIMESTAMP NOT NULL,
    REQDATEDLV        TIMESTAMP NOT NULL,
    REQDATEORDER      TIMESTAMP NOT NULL,
    REQPOSTATUS       INT     NOT NULL,
    VENDGROUPID       STRING  NOT NULL,
    VENDID            STRING  NOT NULL,
    PARTITION         STRING  NOT NULL,
    DATAAREAID        STRING  NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID             LONG    NOT NULL,
    /* PRIMARY KEY (EXECUTIONID, PLANVERSION, REFID, DATAAREAID, PARTITION)
       – Delta Lake does not enforce primary keys.  If you need
       --> uniqueness, create a UNIQUE index or enforce in the application. */
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2. (Optional) Verify the schema was created correctly
# ------------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIReqPOStaging`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near ')'. SQLSTATE: 42601 (line 1, pos 1394)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBIReqPOStaging` (     DEFINITIONGROUP   STRING  NOT NULL,     EXECUTIONID       STRING  NOT NULL,     ISSELECTED        INT     NOT NULL,     TRANSFERSTATUS    INT     NOT NULL,     COVINVENTDIMID    STRING  NOT NULL,     COMPANY           STRING  NOT NULL,     ITEMBUYERGROUPID  STRING  NOT NULL,     ITEMGROUPID       STRING  NOT NULL,     ITEMID            STRING  NOT NULL,     LEADTIME          INT     NOT NULL,     PLANVERSION       LONG    NOT NULL,     PURCHID           STRING  NOT NULL,     PURCHQTY          DECIMAL(32,6) NOT NULL,     PURCHUNIT         STRING  NOT NULL,     QTY               DECIMAL(32,6) NOT NULL,     REFID             STRING  NOT NULL,     REFTYPE           INT     NOT NULL,     REQDATE           TIMESTAMP NOT NULL,     REQDATEDLV        TIMESTAMP NOT NULL,     REQDATEORDER      TIMESTAMP NOT NULL,     REQPOSTATUS       INT     NOT NULL,     VENDGROUPID       STRING  NOT NULL,     VENDID            STRING  NOT NULL,     PARTITION         STRING  NOT NULL,     DATAAREAID        STRING  NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID             LONG    NOT NULL,     /* PRIMARY KEY (EXECUTIONID, PLANVERSION, REFID, DATAAREAID, PARTITION)        – Delta Lake does not enforce primary keys.  If you need        --> uniqueness, create a UNIQUE index or enforce in the application. */ ) USING DELTA
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
