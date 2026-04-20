# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIBudgetRegisterEntryHeaderStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIBudgetRegisterEntryHeaderStaging.Table.sql`

# COMMAND ----------

# -----------------------------------------------------
#  Create the staging table `SMRBIBudgetRegisterEntryHeaderStaging`
# -----------------------------------------------------
# Spark SQL uses a fully‑qualified table name `dbe_dbx_internships.dbo.{table}`.
# Databricks Delta Lake does not support a native PRIMARY KEY constraint,
# so the key definition is documented in a comment.  If you need strict
# uniqueness you should enforce it at the application level or by setting
# Delta Lake's primary‑key properties (available in newer releases).
# -----------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBudgetRegisterEntryHeaderStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    BUDGETCODE STRING NOT NULL,
    BUDGETMODELID STRING NOT NULL,
    DEFAULTDATE TIMESTAMP NOT NULL,
    PRIMARYLEDGERID BIGINT NOT NULL,
    BUDGETMODELTYPE INT NOT NULL,
    BUDGETSUBMODELID STRING NOT NULL,
    TRANSACTIONNUMBER STRING NOT NULL,
    DESCRIPTION STRING NOT NULL,
    COMPANY STRING NOT NULL,
    BUDGETREGISTERRECID BIGINT NOT NULL,
    STATUS INT NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
