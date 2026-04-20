# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjItemTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjItemTransStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------
#  Create staging table `SMRBIProjItemTransStaging`
#
#  The original T‑SQL definition uses a clustered primary key and many 
#  Microsoft‑SQL‑Server specific data types (NVARCHAR, NUMERIC).
#
#  In Databricks/Delta Lake we translate the types to Spark SQL equivalents:
#      NVARCHAR        -> STRING
#      NUMERIC(p,s)    -> DECIMAL(p, s)
#      DATETIME        -> DATE (or TIMESTAMP if you need the time part)
#
#  Delta Lake does not support primary keys, foreign keys or clustered keys.  
#  They are therefore omitted in the CREATE TABLE statement.  
#  If strict uniqueness or ordering is required you can enforce it in
#  downstream processing or use the `uniqueKey` option (experimental).
#
#  The column that was named `[PARTITION]` in T‑SQL is used as a normal
#  column name (you can also partition the table on this column with
#  `PARTITIONED BY (PARTITION)`).  The name is quoted to avoid clashes
#  with the reserved word `PARTITION`.
#
#  Full qualification:  dbe_dbx_internships.dbo.SMRBIProjItemTransStaging
# --------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjItemTransStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID      STRING NOT NULL,
    ISSELECTED       INT NOT NULL,
    TRANSFERSTATUS   INT NOT NULL,
    ACTIVITYNUMBER   STRING NOT NULL,
    CATEGORYID       STRING NOT NULL,
    CURRENCYID       STRING NOT NULL,
    COMPANY          STRING NOT NULL,
    ITEMID           STRING NOT NULL,
    LINEPROPERTYID   STRING NOT NULL,
    PROJID            STRING NOT NULL,
    PROJTRANSID       STRING NOT NULL,
    QTY              DECIMAL(32,6) NOT NULL,
    TOTALCOSTAMOUNTCUR DECIMAL(32,6) NOT NULL,
    TOTALSALESAMOUNTCUR DECIMAL(32,6) NOT NULL,
    TRANSDATE         DATE NOT NULL,
    TXT               STRING NOT NULL,
    PARTITION         STRING NOT NULL,
    DATAAREAID        STRING NOT NULL,
    SYNCSTARTDATETIME DATE NOT NULL
) USING DELTA
-- PARTITIONED BY (PARTITION)  <-- uncomment if you want to physically partition the data
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
