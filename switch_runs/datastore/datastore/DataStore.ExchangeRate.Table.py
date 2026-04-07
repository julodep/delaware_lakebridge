# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ExchangeRate.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ExchangeRate.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣ Create the ExchangeRate table in the target catalog & schema
# ------------------------------------------------------------------
# The original T‑SQL definition uses the following data types:
#   - NVARCHAR / VARCHAR  →  STRING          (Spark/Delta types)
#   - DATETIME           →  TIMESTAMP
#   - NUMERIC(p,s)       →  DECIMAL(p,s)
#
# The statement also specifies `NOT NULL` constraints for the first 5 columns
# and declares the table to be on the default primary partition.  In Delta
# Lake / Databricks we can simply create a Delta table with the same
# column definitions.  Using *CREATE OR REPLACE* guarantees that the
# notebook can be run repeatedly without raising an exception if the
# table already exists.

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`ExchangeRate` (
    ExchangeRateTypeCode STRING NOT NULL,
    ExchangeRateTypeName STRING NOT NULL,
    DataSource STRING NOT NULL,
    FromCurrencyCode STRING NOT NULL,
    ToCurrencyCode STRING NOT NULL,
    ValidFrom TIMESTAMP,
    ValidTo TIMESTAMP,
    ExchangeRate DECIMAL(38, 17)
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
