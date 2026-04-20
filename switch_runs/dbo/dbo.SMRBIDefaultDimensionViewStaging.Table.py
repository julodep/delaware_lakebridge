# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDefaultDimensionViewStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDefaultDimensionViewStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
#  Databricks notebook – Create Δ table SMRBIDefaultDimensionViewStaging
#  Fully‑qualified name: dbe_dbx_internships.dbo.SMRBIDefaultDimensionViewStaging
# ------------------------------------------------------------

# 1. Define the schema in Databricks (Spark SQL / Delta Lake)
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIDefaultDimensionViewStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT     NOT NULL,
    TRANSFERSTATUS INT     NOT NULL,
    DISPLAYVALUE   STRING  NOT NULL,
    DEFAULTDIMENSION BIGINT NOT NULL,
    NAME           STRING  NOT NULL,
    PARTITION      STRING  NOT NULL,
    YSLEGROUPDIMENSION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID          BIGINT NOT NULL
)
USING delta
""")

# COMMAND ----------

# 2. (Optional) Create a primary key view or enforce uniqueness via
# a unique index in the underlying storage system.
# Databricks Delta does not support SQL PRIMARY KEY constraints out of the box,
# so you can keep the key for reference or rely on your application logic
# to maintain uniqueness.

# 3. Verify the table was created
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.SMRBIDefaultDimensionViewStaging").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
