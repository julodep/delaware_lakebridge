# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductCostBreakdownPrice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.ProductCostBreakdownPrice.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the ProductCostBreakdownPrice table in the target catalog and schema
# ------------------------------------------------------------------
#  All table/column names are fully‑qualified: dbe_dbx_internships.datastore.ProductCostBreakdownPrice
#  Data types have been mapped from T‑SQL to Spark SQL per the guidelines:
#   * NVARCHAR => STRING
#   * NUMERIC(p,s) => DECIMAL(p,s)
#   * BIGINT   => BIGINT (Spark’s 64‑bit integer type)
#   * DATETIME => TIMESTAMP
#   * VARCHAR(3) NOT NULL  => STRING NOT NULL
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`ProductCostBreakdownPrice` (
    ItemNumber           STRING NOT NULL,
    InventDimCode        STRING NOT NULL,
    UnitCode             STRING NOT NULL,
    CompanyCode          STRING NOT NULL,
    PriceCalcId          STRING NOT NULL,
    Price                DECIMAL(38,17),           -- NUMERIC(38,17) in T‑SQL
    VersionCode          STRING NOT NULL,
    PriceType            STRING,
    StartValidityDate    TIMESTAMP NOT NULL,       -- DATETIME in T‑SQL
    EndValidityDate      TIMESTAMP,
    CalculationNr        STRING,
    CalculationNrTech    BIGINT,
    IsMaxCalculation     STRING NOT NULL,          -- VARCHAR(3) NOT NULL
    IsActivePrice        STRING NOT NULL,
    IsMaxPrice           STRING NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 790)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`ProductCostBreakdownPrice` (     ItemNumber           STRING NOT NULL,     InventDimCode        STRING NOT NULL,     UnitCode             STRING NOT NULL,     CompanyCode          STRING NOT NULL,     PriceCalcId          STRING NOT NULL,     Price                DECIMAL(38,17),           -- NUMERIC(38,17) in T‑SQL     VersionCode          STRING NOT NULL,     PriceType            STRING,     StartValidityDate    TIMESTAMP NOT NULL,       -- DATETIME in T‑SQL     EndValidityDate      TIMESTAMP,     CalculationNr        STRING,     CalculationNrTech    BIGINT,     IsMaxCalculation     STRING NOT NULL,          -- VARCHAR(3) NOT NULL     IsActivePrice        STRING NOT NULL,     IsMaxPrice           STRING NOT NULL );
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
