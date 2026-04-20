# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjBudgetRevisionLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjBudgetRevisionLineStaging.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------
# Databricks notebook – create the staging table
# ----------------------------------------------------------
# (Replace `dbe_dbx_internships` and `dbo` with your actual
# catalog and schema names before running the notebook.)

# ---------- 1. Create the table ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.SMRBIProjBudgetRevisionLineStaging (
    DEFINITIONGROUP STRING COMMENT 'NVARCHAR(60) NOT NULL',
    EXECUTIONID   STRING COMMENT 'NVARCHAR(90) NOT NULL',
    ISSELECTED    INT    COMMENT 'INT NOT NULL',
    TRANSFERSTATUS INT    COMMENT 'INT NOT NULL',
    NEWTOTALBUDGET DECIMAL(32,6) COMMENT 'NUMERIC(32,6) NOT NULL',
    PREVIOUSAPPROVEDBUDGET DECIMAL(32,6) COMMENT 'NUMERIC(32,6) NOT NULL',
    PROJBUDGETLINE BIGINT COMMENT 'BIGINT NOT NULL',
    PROJBUDGETREVISION BIGINT COMMENT 'BIGINT NOT NULL',
    REVISIONAMOUNT DECIMAL(32,6) COMMENT 'NUMERIC(32,6) NOT NULL',
    PARTITION STRING COMMENT 'NVARCHAR(20) NOT NULL',
    DATAAREAID STRING COMMENT 'NVARCHAR(4) NOT NULL',
    SYNCSTARTDATETIME TIMESTAMP COMMENT 'DATETIME NOT NULL',
    RECID BIGINT COMMENT 'BIGINT NOT NULL'
)
USING DELTA
""")

# COMMAND ----------

# ---------- 2. Add a logical primary‑key hint ----------
# Delta Lake does not enforce PRIMARY KEY constraints, but we can
# create a unique index by adding a unique key property or by
# indexing the columns in downstream queries.  The PK definition
# is preserved in the comment for reference.
spark.sql(f"""
ALTER TABLE `dbe_dbx_internships`.`dbo`.SMRBIProjBudgetRevisionLineStaging
SET TBLPROPERTIES (
    'primaryKey' = 'EXECUTIONID, PROJBUDGETLINE, PROJBUDGETREVISION, DATAAREAID, PARTITION'
)
""")

# COMMAND ----------

# ---------- 3. Verify the creation ----------
table_df = spark.sql(f"SELECT * FROM `dbe_dbx_internships`.`dbo`.SMRBIProjBudgetRevisionLineStaging LIMIT 1")
table_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
