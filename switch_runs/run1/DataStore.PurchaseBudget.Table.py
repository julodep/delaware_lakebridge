# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PurchaseBudget.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.PurchaseBudget.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required libraries (Databricks provides Spark)
# ------------------------------------------------------------

# No additional imports are needed for executing SQL via spark.sql

# ------------------------------------------------------------
# Ensure the target database exists and switch to it
# ------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS DataStore")
spark.sql("USE DataStore")

# COMMAND ----------

# ------------------------------------------------------------
# Create the PurchaseBudget table in the DataStore schema as a Delta table
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS PurchaseBudget (
    ProductCode               STRING,
    ForecastModelCode         STRING,
    CompanyCode               STRING,
    SupplierCode              STRING,
    DefaultExchangeRateTypeCode STRING,
    BudgetExchangeRateTypeCode STRING,
    TransactionCurrencyCode   STRING,
    AccountingCurrencyCode    STRING,
    ReportingCurrencyCode     STRING,
    GroupCurrencyCode         STRING,
    DefaultDimension          BIGINT,
    InventDimCode             STRING,
    BudgetDate                TIMESTAMP,
    PurchaseUnit              STRING,
    BudgetQuantity            DECIMAL(32,6),
    PurchUnitPriceTC          DECIMAL(32,6),
    PurchUnitPriceAC          DECIMAL(38,6),
    PurchUnitPriceRC          DECIMAL(38,6),
    PurchUnitPriceGC          DECIMAL(38,6),
    PurchUnitPriceAC_Budget   DECIMAL(38,6),
    PurchUnitPriceRC_Budget   DECIMAL(38,6),
    PurchUnitPriceGC_Budget   DECIMAL(38,6),
    BudgetAmountTC            DECIMAL(32,6),
    BudgetAmountAC            DECIMAL(38,6),
    BudgetAmountRC            DECIMAL(38,6),
    BudgetAmountGC            DECIMAL(38,6),
    BudgetAmountAC_Budget     DECIMAL(38,6),
    BudgetAmountRC_Budget     DECIMAL(38,6),
    BudgetAmountGC_Budget     DECIMAL(38,6),
    AppliedExchangeRateTC     DECIMAL(38,6),
    AppliedExchangeRateRC     DECIMAL(38,21),
    AppliedExchangeRateAC     DECIMAL(38,21),
    AppliedExchangeRateGC     DECIMAL(38,21),
    AppliedExchangeRateRC_Budget DECIMAL(38,21),
    AppliedExchangeRateAC_Budget DECIMAL(38,21),
    AppliedExchangeRateGC_Budget DECIMAL(38,21)
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------
# Optional: Verify that the table was created successfully
# ------------------------------------------------------------
created_df = spark.sql("SHOW TABLES IN DataStore LIKE 'PurchaseBudget'")
display(created_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
