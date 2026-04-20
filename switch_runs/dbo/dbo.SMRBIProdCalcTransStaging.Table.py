# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProdCalcTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProdCalcTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the SMRBIProdCalcTransStaging table in the target Delta table
# ------------------------------------------------------------------
# All object names are fully‑qualified: `dbe_dbx_internships`.`dbo`.`SMRBIProdCalcTransStaging`
# Data types are mapped from T‑SQL to Spark SQL as follows:
#   nvarchar       -> STRING
#   numeric(p,s)   -> DECIMAL(p,s)
#   int            -> INT
#   bigint         -> BIGINT
#   datetime       -> TIMESTAMP
#
# Primary key constraints are not supported in Delta Lake, so the
# PK defined in the original T‑SQL is omitted.  Add logic in your
# application or catalog metadata if uniqueness enforcement is required.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProdCalcTransStaging` (
    DEFINITIONGROUP     STRING   NOT NULL,
    EXECUTIONID         STRING   NOT NULL,
    ISSELECTED          INT      NOT NULL,
    TRANSFERSTATUS      INT      NOT NULL,
    BOM                 INT      NOT NULL,
    CALCTYPE            INT      NOT NULL,
    COLLECTREFLEVEL     INT      NOT NULL,
    CONSUMPCONSTANT     DECIMAL(32,16) NOT NULL,
    CONSUMPTYPE         INT      NOT NULL,
    CONSUMPVARIABLE     DECIMAL(32,16) NOT NULL,
    COSTAMOUNT          DECIMAL(32,16) NOT NULL,
    COSTGROUPID         STRING   NOT NULL,
    COSTMARKUP          DECIMAL(32,16) NOT NULL,
    COMPANY             STRING   NOT NULL,
    IDREFRECID          BIGINT   NOT NULL,
    IDREFTABLEID        INT      NOT NULL,
    INVENTDIMID         STRING   NOT NULL,
    LINENUM             DECIMAL(32,16) NOT NULL,
    OPRID               STRING   NOT NULL,
    OPRNUM              INT      NOT NULL,
    QTY                 DECIMAL(32,6)  NOT NULL,
    REALCONSUMP         DECIMAL(32,16) NOT NULL,
    REALCOSTADJUSTMENT  DECIMAL(32,16) NOT NULL,
    REALCOSTAMOUNT      DECIMAL(32,16) NOT NULL,
    REALQTY             DECIMAL(32,16) NOT NULL,
    RESOURCE_           STRING   NOT NULL,
    SALESAMOUNT         DECIMAL(32,6)  NOT NULL,
    SALESMARKUP         DECIMAL(32,6)  NOT NULL,
    TRANSDATE           TIMESTAMP NOT NULL,
    TRANSREFID          STRING   NOT NULL,
    TRANSREFTYPE        INT      NOT NULL,
    UNITID              STRING   NOT NULL,
    VENDID              STRING   NOT NULL,
    PRODCALCTRANSRECID  BIGINT   NOT NULL,
    PARTITION           STRING   NOT NULL,
    SYNCSTARTDATETIME   TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
