# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.RFQ.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.RFQ.Table.sql`

# COMMAND ----------

# -------------------------------------------------
# Ensure we are operating in the default Unity Catalog.
# Adjust the catalog name if your environment uses a custom one.
# -------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# -------------------------------------------------
# Setup: ensure the target schema (database) exists.
# Use lowercase name to match the permissions error message.
# -------------------------------------------------
spark.sql("""
CREATE DATABASE IF NOT EXISTS `datastore`
""")

# COMMAND ----------

# -------------------------------------------------
# Create the RFQ table in the `datastore` schema.
# The original T‑SQL script used brackets, SET options,
# and the ON [PRIMARY] clause, which are not applicable in Databricks.
# Those parts are omitted or added as comments below.
# -------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE `datastore`.`RFQ` (
    RFQCaseCode                STRING      NOT NULL,
    RFQCaseName                STRING      NOT NULL,
    RFQCaseLineNumber          BIGINT      NOT NULL,
    RFQCode                    STRING      NOT NULL,
    RFQName                    STRING      NOT NULL,
    RFQLineNumber              DECIMAL(32,16) NOT NULL,
    SupplierCode               STRING      NOT NULL,
    CompanyCode                STRING      NOT NULL,
    ProductConfigurationCode   STRING      NOT NULL,
    ProductCode                STRING      NOT NULL,
    DeliveryModeCode           STRING      NOT NULL,
    DeliveryTermsCode          STRING      NOT NULL,
    DefaultExchangeRateTypeCode STRING     NOT NULL,
    BudgetExchangeRateTypeCode STRING      NOT NULL,
    TransactionCurrencyCode    STRING      NOT NULL,
    AccountingCurrencyCode     STRING      NOT NULL,
    ReportingCurrencyCode      STRING      NOT NULL,
    GroupCurrencyCode          STRING      NOT NULL,
    DeliveryDate               DATE,
    ExpiryDate                 DATE,
    CreatedDate                DATE,
    RFQCaseStatusLow          STRING,
    RFQCaseStatusHigh         STRING,
    RFQLineStatus             STRING,
    PurchaseUnit              STRING      NOT NULL,
    PurchQuantity             DECIMAL(32,6) NOT NULL,
    PurchPriceTC              DECIMAL(32,6) NOT NULL,
    PurchPriceAC              DECIMAL(38,6) NOT NULL,
    PurchPriceRC              DECIMAL(38,6) NOT NULL,
    PurchPriceGC              DECIMAL(38,6) NOT NULL,
    PurchPriceAC_Budget       DECIMAL(38,6) NOT NULL,
    PurchPriceRC_Budget       DECIMAL(38,6) NOT NULL,
    PurchPriceGC_Budget       DECIMAL(38,6) NOT NULL,
    RFQLineAmountTC           DECIMAL(32,6) NOT NULL,
    RFQLineAmountAC           DECIMAL(38,6) NOT NULL,
    RFQLineAmountRC           DECIMAL(38,6) NOT NULL,
    RFQLineAmountGC           DECIMAL(38,6) NOT NULL,
    RFQLineAmountAC_Budget    DECIMAL(38,6) NOT NULL,
    RFQLineAmountRC_Budget    DECIMAL(38,6) NOT NULL,
    RFQLineAmountGC_Budget    DECIMAL(38,6) NOT NULL,
    CostAvoidanceAmountTC    DECIMAL(33,6) NOT NULL,
    CostAvoidanceAmountAC    DECIMAL(38,6) NOT NULL,
    CostAvoidanceAmountRC    DECIMAL(38,6) NOT NULL,
    CostAvoidanceAmountGC    DECIMAL(38,6) NOT NULL,
    CostAvoidanceAmountAC_Budget DECIMAL(38,6) NOT NULL,
    CostAvoidanceAmountRC_Budget DECIMAL(38,6) NOT NULL,
    CostAvoidanceAmountGC_Budget DECIMAL(38,6) NOT NULL,
    MaxPurchPriceTC           DECIMAL(32,6) NOT NULL,
    MaxPurchPriceAC           DECIMAL(38,6) NOT NULL,
    MaxPurchPriceRC           DECIMAL(38,6) NOT NULL,
    MaxPurchPriceGC           DECIMAL(38,6) NOT NULL,
    MaxPurchPriceAC_Budget    DECIMAL(38,6) NOT NULL,
    MaxPurchPriceRC_Budget    DECIMAL(38,6) NOT NULL,
    MaxPurchPriceGC_Budget    DECIMAL(38,6) NOT NULL,
    MinPurchPriceTC           DECIMAL(32,6) NOT NULL,
    MinPurchPriceAC           DECIMAL(38,6) NOT NULL,
    MinPurchPriceRC           DECIMAL(38,6) NOT NULL,
    MinPurchPriceGC           DECIMAL(38,6) NOT NULL,
    MinPurchPriceAC_Budget    DECIMAL(38,6) NOT NULL,
    MinPurchPriceRC_Budget    DECIMAL(38,6) NOT NULL,
    MinPurchPriceGC_Budget    DECIMAL(38,6) NOT NULL,
    AppliedExchangeRateTC     DECIMAL(38,6),
    AppliedExchangeRateRC     DECIMAL(38,21) NOT NULL,
    AppliedExchangeRateAC     DECIMAL(38,21) NOT NULL,
    AppliedExchangeRateGC     DECIMAL(38,21) NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 2: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
