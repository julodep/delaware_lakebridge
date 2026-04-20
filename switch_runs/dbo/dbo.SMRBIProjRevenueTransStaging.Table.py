# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjRevenueTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjRevenueTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# 1️⃣  Create the staging table `SMRBIProjRevenueTransStaging`
# ------------------------------------------------------------
# Databricks Delta tables in a Unity‑Catalog context are created with a
# fully‑qualified format:  `dbe_dbx_internships.dbo.SMRBIProjRevenueTransStaging`.
#
# The T‑SQL definition uses a custom PRIMARY KEY and several table options.
# Delta Lake does not support all T‑SQL constraints natively, so the
# primary‑key definition is omitted (you can add a comment or use custom
# tooling if a primary key is required).  Only the column definitions and
# data types are preserved.  Numeric(32,6) maps to DECIMAL(32,6).
#
# Uncomment the DROP line if you want to force a fresh table each run.
# ------------------------------------------------------------
# spark.sql("DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.SMRBIProjRevenueTransStaging")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.SMRBIProjRevenueTransStaging (
    DEFINITIONGROUP   STRING,
    EXECUTIONID       STRING,
    ISSELECTED        INT,
    TRANSFERSTATUS    INT,
    CATEGORYID        STRING,
    CURRENCYID        STRING,
    COMPANY           STRING,
    LINEPROPERTYID    STRING,
    PROJID            STRING,
    QTY               DECIMAL(32,6),
    TOTALSALESAMOUNTCUR DECIMAL(32,6),
    TRANSDATE         TIMESTAMP,
    TRANSID           STRING,
    TXT               STRING,
    PARTITION         STRING,
    DATAAREAID        STRING,
    SYNCSTARTDATETIME TIMESTAMP
)
USING delta
""")

# COMMAND ----------

# ------------------------------------------------------------
# 2️⃣  Verify the table was created (optional)
# ------------------------------------------------------------
spark.sql(f"SHOW TABLES IN `dbe_dbx_internships`.`dbo` LIKE 'SMRBIProjRevenueTransStaging'").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
