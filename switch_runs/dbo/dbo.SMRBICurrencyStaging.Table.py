# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICurrencyStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICurrencyStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks Notebook: Create the SMRBICurrencyStaging table
# ------------------------------------------------------------------
#
# This code translates the T‑SQL `CREATE TABLE` statement to Databricks.
# Primary key constraint is omitted because Delta Lake does not enforce it.
#
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICurrencyStaging`
(
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CURRENCYCODE STRING NOT NULL,
    NAME STRING NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Verify table creation
# ------------------------------------------------------------------
display(spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBICurrencyStaging`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
