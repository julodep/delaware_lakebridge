# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBISystemUserStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBISystemUserStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Create the staging table SMRBISystemUserStaging
# -------------------------------------------------------------
# 1. Define the catalog and schema that the table belongs to.
#    Replace the placeholder strings with your actual catalog and schema names
#    before running the notebook.
catalog = "your_catalog"
schema  = "your_schema"

# COMMAND ----------

# 2. Build the CREATE TABLE statement.
create_table_sql = f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBISystemUserStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT    NOT NULL,
    TRANSFERSTATUS INT    NOT NULL,
    USERID         STRING NOT NULL,
    USERNAME       STRING NOT NULL,
    COMPANY        STRING NOT NULL,
    NETWORKDOMAIN  STRING NOT NULL,
    ALIAS          STRING NOT NULL,
    PARTITION      STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
"""
spark.sql(create_table_sql)

# COMMAND ----------

# -------------------------------------------------------------
# Verify that the table has been created correctly
# -------------------------------------------------------------
df = spark.table(f"dbe_dbx_internships.dbo.SMRBISystemUserStaging")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
