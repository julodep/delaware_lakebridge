# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductCostBreakdownPrice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ProductCostBreakdownPrice.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: No additional imports required; Databricks provides `spark`
# ------------------------------------------------------------

# The following T‑SQL session settings have no direct equivalent in Databricks.
# They are ignored, but we keep them as comments for reference.
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ------------------------------------------------------------
# Create the Delta table in the default (or accessible) database
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE ProductCostBreakdownPrice (
    ItemNumber           STRING   NOT NULL,
    InventDimCode        STRING   NOT NULL,
    UnitCode             STRING   NOT NULL,
    CompanyCode          STRING   NOT NULL,
    PriceCalcId          STRING   NOT NULL,
    Price                DECIMAL(38,17),
    VersionCode          STRING   NOT NULL,
    PriceType            STRING,
    StartValidityDate    TIMESTAMP NOT NULL,
    EndValidityDate      TIMESTAMP,
    CalculationNr        STRING,
    CalculationNrTech   LONG,
    IsMaxCalculation     STRING   NOT NULL,
    IsActivePrice        STRING   NOT NULL,
    IsMaxPrice           STRING   NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
