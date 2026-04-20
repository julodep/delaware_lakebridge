# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICostControlTransCommittedCostStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICostControlTransCommittedCostStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook: create_SMrbicostcontrol_trans_committedcoststaging.py
#
# Purpose
# -------
# Transpiles the T‑SQL `CREATE TABLE` statement for
#   dbo.SMRBICostControlTransCommittedCostStaging
# into a fully‑qualified Delta Lake table definition that runs on a
# Databricks cluster.
#
# Notes on Data‑type Mapping
# --------------------------
# T‑SQL   -> Spark SQL
# --------------------------------------
# nvarchar(60)  -> STRING
# nvarchar(90)  -> STRING
# int           -> INT
# datetime      -> TIMESTAMP
# numeric(32,6) -> DECIMAL(32,6)
#
# Notes on Constraints
# --------------------
# Delta Lake (i.e., the underlying Spark catalog) does not support
#   PRIMARY KEY or clustering directly.  The primary‑key columns are
#   listed as normal columns; if you need uniqueness guarantees you
#   would enforce them at the application layer or through a
#   Delta Lake transaction.
#
# ------------------------------------------------------------------
# (1) Create the table using a fully‑qualified path
#     `dbe_dbx_internships`.`dbo`.`SMRBICostControlTransCommittedCostStaging`
# ------------------------------------------------------------------
catalog = "my_catalog"          # <-- replace with your catalog name
schema  = "my_schema"           # <-- replace with your schema name

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICostControlTransCommittedCostStaging`
  (
      DEFINITIONGROUP   STRING   NOT NULL,
      EXECUTIONID       STRING   NOT NULL,
      ISSELECTED        INT      NOT NULL,
      TRANSFERSTATUS    INT      NOT NULL,
      ACTIVITYNUMBER    STRING   NOT NULL,
      AMOUNT            DECIMAL(32,6) NOT NULL,
      CATEGORYID        STRING   NOT NULL,
      COMMITTEDCOSTORIG INT      NOT NULL,
      COMMITTEDDATE     TIMESTAMP NOT NULL,
      CURRENCYCODE      STRING   NOT NULL,
      COMPANY           STRING   NOT NULL,
      EMPLITEMID        STRING   NOT NULL,
      OPEN_              INT      NOT NULL,
      PROJID            STRING   NOT NULL,
      PROJTRANSID        STRING   NOT NULL,
      QTY               DECIMAL(32,6) NOT NULL,
      VOUCHER           STRING   NOT NULL,
      PARTITION         STRING   NOT NULL,
      DATAAREAID        STRING   NOT NULL,
      SYNCSTARTDATETIME TIMESTAMP NOT NULL,
      RECID             BIGINT   NOT NULL
  )
  USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# (2) Add a comment about the historic PK information
# ------------------------------------------------------------------
# The original T‑SQL defined a clustered primary key on the columns
#   (EXECUTIONID, PROJTRANSID, DATAAREAID, PARTITION).
# In Delta Lake we cannot create that constraint, but we capture it in
#   the table metadata for reference.
spark.sql(f"""
  ALTER TABLE `dbe_dbx_internships`.`dbo`.`SMRBICostControlTransCommittedCostStaging`
  SET TBLPROPERTIES (
      'primaryKey' = 'EXECUTIONID,PROJTRANSID,DATAAREAID,PARTITION'
  )
""")

# COMMAND ----------

# ------------------------------------------------------------------
# (3) Verify the table was created
# ------------------------------------------------------------------
tables_df = spark.sql(f"SHOW TABLES IN `dbe_dbx_internships`.`dbo`")
tables_df.filter("tableName = 'SMRBICostControlTransCommittedCostStaging'").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
