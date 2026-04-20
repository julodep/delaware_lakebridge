# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIEcoResProductTranslationStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIEcoResProductTranslationStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook code – create the staging table
# ------------------------------------------------------------------
# Replace dbe_dbx_internships and dbo with the actual catalog and schema names.
catalog = "my_catalog"     # <-- change to your catalog name
schema  = "my_schema"      # <-- change to your schema name

# COMMAND ----------

# Data‑type mapping:
#   NVARCHAR(_)  -> STRING
#   INT          -> INT
#   BIGINT       -> BIGINT
#   DATETIME     -> TIMESTAMP
#
# Primary‑key constraints are not enforced by Delta Lake.
# The key columns (EXECUTIONID, PRODUCTNUMBER, LANGUAGEID, PARTITION) are
# listed in a comment for reference only.

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIEcoResProductTranslationStaging (
    DEFINITIONGROUP   STRING  NOT NULL,
    EXECUTIONID       STRING  NOT NULL,
    ISSELECTED        INT     NOT NULL,
    TRANSFERSTATUS    INT     NOT NULL,
    PRODUCTNUMBER     STRING  NOT NULL,
    LANGUAGEID        STRING  NOT NULL,
    PRODUCTNAME       STRING  NOT NULL,
    DESCRIPTION       STRING  NOT NULL,
    PARTITION         STRING  NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID             BIGINT  NOT NULL
) USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: Verify that the table was created correctly
# ------------------------------------------------------------------
result = spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.SMRBIEcoResProductTranslationStaging")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
