# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesBudget.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.SalesBudget.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Setup: import any required libraries (Databricks provides Spark session)
# ------------------------------------------------------------------
# No additional imports are necessary for executing Spark SQL statements.

# ------------------------------------------------------------------
# NOTE: The original script contained SET options and a GO batch separator,
# which are specific to SQL Server and have no effect in Databricks.
# They are therefore omitted (or left as comments for reference).
# ------------------------------------------------------------------
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# ------------------------------------------------------------------
# Create the SalesBudget table.
# Use the default database (or any database the current user has access to)
# instead of the `DataStore` schema, which caused a permissions error.
# ------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE SalesBudget (
    `Comment`                     STRING      NOT NULL,
    `CompanyCode`                 STRING,
    `ProductCode`                 STRING      NOT NULL,
    `ProductGroupCode`            STRING      NOT NULL,
    `CustomerCode`                STRING      NOT NULL,
    `CustomerGroupCode`           STRING      NOT NULL,
    `ForecastModelCode`           STRING      NOT NULL,
    `InventDimCode`               STRING      NOT NULL,
    `DefaultDimension`            LONG        NOT NULL,
    `DefaultExchangeRateTypeCode` STRING      NOT NULL,
    `BudgetExchangeRateTypeCode`  STRING      NOT NULL,
    `TransactionCurrencyCode`     STRING,
    `AccountingCurrencyCode`      STRING      NOT NULL,
    `ReportingCurrencyCode`       STRING      NOT NULL,
    `GroupCurrencyCode`           STRING      NOT NULL,
    `ForecastDate`                TIMESTAMP   NOT NULL,
    `SalesUnit`                   STRING      NOT NULL,
    `ForecastQuantity`            DECIMAL(32,6) NOT NULL,
    `GrossSalesAmountTC`          DECIMAL(32,6) NOT NULL,
    `GrossSalesAmountAC`          DECIMAL(38,6) NOT NULL,
    `GrossSalesAmountRC`          DECIMAL(38,6) NOT NULL,
    `GrossSalesAmountGC`          DECIMAL(38,6) NOT NULL,
    `GrossSalesAmountAC_Budget`   DECIMAL(38,6) NOT NULL,
    `GrossSalesAmountRC_Budget`   DECIMAL(38,6) NOT NULL,
    `GrossSalesAmountGC_Budget`   DECIMAL(38,6) NOT NULL,
    `CostPriceTC`                 DECIMAL(38,6) NOT NULL,
    `CostPriceAC`                 DECIMAL(38,6) NOT NULL,
    `CostPriceRC`                 DECIMAL(38,6) NOT NULL,
    `CostPriceGC`                 DECIMAL(38,6) NOT NULL,
    `CostPriceAC_Budget`          DECIMAL(38,6) NOT NULL,
    `CostPriceRC_Budget`          DECIMAL(38,6) NOT NULL,
    `CostPriceGC_Budget`          DECIMAL(38,6) NOT NULL,
    `GrossMarginTC`               DECIMAL(38,6),
    `GrossMarginAC`               DECIMAL(38,6) NOT NULL,
    `GrossMarginRC`               DECIMAL(38,6) NOT NULL,
    `GrossMarginGC`               DECIMAL(38,6) NOT NULL,
    `GrossMarginAC_Budget`        DECIMAL(38,6) NOT NULL,
    `GrossMarginRC_Budget`        DECIMAL(38,6) NOT NULL,
    `GrossMarginGC_Budget`        DECIMAL(38,6) NOT NULL,
    `AppliedExchangeRateTC`       DECIMAL(38,6),
    `AppliedExchangeRateRC`       DECIMAL(38,21) NOT NULL,
    `AppliedExchangeRateAC`       DECIMAL(38,21) NOT NULL,
    `AppliedExchangeRateGC`       DECIMAL(38,21) NOT NULL,
    `AppliedExchangeRateRC_Budget`DECIMAL(38,21) NOT NULL,
    `AppliedExchangeRateAC_Budget`DECIMAL(38,21) NOT NULL,
    `AppliedExchangeRateGC_Budget`DECIMAL(38,21) NOT NULL
) USING DELTA
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
