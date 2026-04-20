# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIBudgetModelStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIBudgetModelStaging.Table.sql`

# COMMAND ----------

# Create or replace the table in Delta Lake format
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBudgetModelStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID STRING,
    ISSELECTED INT,
    TRANSFERSTATUS INT,
    BUDGETMODEL STRING,
    NAME STRING,
    COMPANY STRING,
    BUDGETMODELRECID LONG,
    PARTITION STRING,
    DATAAREAID STRING,
    SYNCSTARTDATETIME TIMESTAMP
)
USING delta
""")

# COMMAND ----------

# Show the newly created table schema in a Databricks notebook
display(spark.table(f"dbe_dbx_internships.dbo.SMRBIBudgetModelStaging"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
