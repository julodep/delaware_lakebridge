# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ExchangeRate.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260324104824-3bci/DataStore.ExchangeRate.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Setup: import any required modules (Databricks provides `spark` by default)
# -------------------------------------------------------------------------
# No additional imports are needed for this table creation.

# -------------------------------------------------------------------------
# NOTE: The original T‑SQL script contains `SET ANSI_NULLS ON`,
# `SET QUOTED_IDENTIFIER ON`, and `ON [PRIMARY]` which have no effect in
# Databricks / Delta Lake. They are therefore omitted (commented out below).
# -------------------------------------------------------------------------
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# -------------------------------------------------------------------------
# Create the ExchangeRate table in the target catalog and schema.
# Using `CREATE OR REPLACE TABLE` ensures the operation is idempotent.
# -------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.ExchangeRate (
    ExchangeRateTypeCode STRING NOT NULL,
    ExchangeRateTypeName STRING NOT NULL,
    DataSource STRING NOT NULL,
    FromCurrencyCode STRING NOT NULL,
    ToCurrencyCode STRING NOT NULL,
    ValidFrom TIMESTAMP,
    ValidTo TIMESTAMP,
    ExchangeRate DECIMAL(38, 17)
)
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
