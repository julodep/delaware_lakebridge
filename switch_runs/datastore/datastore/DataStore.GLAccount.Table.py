# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.GLAccount.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.GLAccount.Table.sql`

# COMMAND ----------

# -----------------------------
# Create the GLAccount table in Databricks
# -----------------------------
# Use fully‑qualified names for catalog, schema, and table.
# All SQL Server data types have been mapped to Spark SQL types.
# Comments within the column definitions have been removed to avoid
# parsing issues in Spark SQL.

# Ensure that the variables `catalog` and `schema` are defined earlier
# in the notebook, e.g.:
# catalog = "DataStore"
# schema  = "dbo"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`datastore`.`GLAccount` (
    CompanyCode                      STRING,
    GLAccountId                      BIGINT,
    GLAccountCode                     STRING,
    GLAccountName                     STRING,
    GLAccountType                     STRING,
    ChartOfAccountsName               STRING,
    MainAccountCategory               STRING,
    MainAccountCategoryDescription    STRING,
    MainAccountCategoryCodeDescription STRING,
    MainAccountCategorySort           INT,
    IsRevenueFlag                     BOOLEAN,
    IsGrossProfitFlag                 BOOLEAN
) USING DELTA;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
