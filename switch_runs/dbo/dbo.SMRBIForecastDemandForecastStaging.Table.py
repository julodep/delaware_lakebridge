# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIForecastDemandForecastStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIForecastDemandForecastStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# 1.  Define the target catalog and schema.  Replace the
#     placeholders `dbe_dbx_internships` and `dbo` with your
#     actual Databricks Unity Catalog catalog and schema names
#     before running this notebook.
# --------------------------------------------------------------
catalog = "dbe_dbx_internships"
schema  = "dbo"

# COMMAND ----------

# --------------------------------------------------------------
# 2.  Create a persistent Delta table that mirrors the T‑SQL
#     definition of dbo.SMRBIForecastDemandForecastStaging.
#     All column names and data types have been translated
#     to Spark SQL equivalents:
#        • NVARCHAR → STRING
#        • DATETIME  → TIMESTAMP
#        • NUMERIC(p,s) → DECIMAL(p,s)
#        • BIGINT   → BIGINT
#     The primary‑key constraint has been omitted because
#     Delta Lake does not enforce primary keys.  If you
#     need uniqueness guarantees, you should add a unique
#     index or use a staging table followed by a deterministic
#     deduplication step.
# --------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIForecastDemandForecastStaging (
    DEFINITIONGROUP          STRING,
    EXECUTIONID              STRING,
    ISSELECTED               INT,
    TRANSFERSTATUS           INT,
    EXPANDID                BIGINT,
    FORECASTMODELID          STRING,
    FORECASTSTARTDATE        TIMESTAMP,
    FORECASTEDQUANTITY       DECIMAL(32, 6),
    FORECASTEDUNITPRICE      DECIMAL(32, 6),
    FORECASTEDREVENUE        DECIMAL(32, 6),
    QUANTITYUNITSYMBOL       STRING,
    PRICINGCURRENCYCODE      STRING,
    CUSTOMERACCOUNTNUMBER    STRING,
    CUSTOMERGROUPID          STRING,
    ITEMNUMBER               STRING,
    KEYID                    STRING,
    STARTDATE                TIMESTAMP,
    INVENTDIMID              STRING,
    COMMENT_                 STRING,
    ITEMGROUPID              STRING,
    FORECASTDEMANDFORECASTDIMENSION BIGINT,
    PARTITION                STRING,
    DATAAREAID               STRING,
    SYNCSTARTDATETIME        TIMESTAMP,
    RECID                    BIGINT
)
USING DELTA
""")

# COMMAND ----------

# --------------------------------------------------------------
# 3.  Verification: show the schema that has just been created.
# --------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.SMRBIForecastDemandForecastStaging").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
