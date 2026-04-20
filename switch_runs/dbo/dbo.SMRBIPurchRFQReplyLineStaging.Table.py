# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPurchRFQReplyLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPurchRFQReplyLineStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# 1. Create the staging table in the target catalog/schema.
# -------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIPurchRFQReplyLineStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID STRING,
    ISSELECTED INT,
    TRANSFERSTATUS INT,
    PURCHRFQCREATEDDATETIME TIMESTAMP,
    COMPANY STRING,
    LINEAMOUNT DECIMAL(32,6),
    LINEPERCENT DECIMAL(32,6),
    PURCHPRICE DECIMAL(32,6),
    PURCHQTY DECIMAL(32,6),
    PURCHUNIT STRING,
    RFQID STRING,
    LINENUM DECIMAL(32,16),
    PARTITION STRING,
    DATAAREAID STRING,
    SYNCSTARTDATETIME TIMESTAMP
);
""")

# COMMAND ----------

# -------------------------------------------------------------
# 2. Verify that the table exists and view the schema.
# -------------------------------------------------------------
spark.sql(f"DESCRIBE EXTENDED `dbe_dbx_internships`.`dbo`.`SMRBIPurchRFQReplyLineStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
