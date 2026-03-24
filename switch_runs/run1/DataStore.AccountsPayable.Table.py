# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.AccountsPayable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.AccountsPayable.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Create the AccountsPayable Delta table in a schema where the user has privileges.
# In Databricks the default database is usually `default`. We switch to it first.
# ------------------------------------------------------------

# Switch to a database where the user has READ/WRITE permissions (e.g., `default`).
spark.sql("USE default")

# COMMAND ----------

# Create or replace the AccountsPayable table in the current database.
spark.sql("""
CREATE OR REPLACE TABLE `AccountsPayable` (
    `AccountsPayableCodeScreening` STRING NOT NULL,
    `RecId` LONG NOT NULL,
    `PurchaseInvoiceCode` STRING NOT NULL,
    `PayablesVoucher` STRING NOT NULL,
    `Description` STRING NOT NULL,
    `CompanyCode` STRING NOT NULL,
    `DimIsOpenAmountId` INT NOT NULL,
    `OutStandingPeriodCode` STRING NOT NULL,
    `SupplierCode` STRING NOT NULL,
    `TransactionCurrencyCode` STRING,
    `AccountingCurrencyCode` STRING NOT NULL,
    `ReportingCurrencyCode` STRING NOT NULL,
    `GroupCurrencyCode` STRING NOT NULL,
    `InvoiceDate` TIMESTAMP NOT NULL,
    `DueDate` TIMESTAMP,
    `LastPaymentDate` TIMESTAMP NOT NULL,
    `DocumentDate` TIMESTAMP,
    `ReportDate` INT,
    `InvoiceAmountTC` DECIMAL(22,6),
    `InvoiceAmountAC` DECIMAL(22,6),
    `InvoiceAmountRC` DECIMAL(38,6) NOT NULL,
    `InvoiceAmountGC` DECIMAL(38,6) NOT NULL,
    `InvoiceAmountAC_Budget` DECIMAL(38,6) NOT NULL,
    `InvoiceAmountRC_Budget` DECIMAL(38,6) NOT NULL,
    `InvoiceAmountGC_Budget` DECIMAL(38,6) NOT NULL,
    `PaidAmountTC` DECIMAL(38,6),
    `PaidAmountAC` DECIMAL(38,6),
    `PaidAmountRC` DECIMAL(38,6) NOT NULL,
    `PaidAmountGC` DECIMAL(38,6) NOT NULL,
    `PaidAmountAC_Budget` DECIMAL(38,6) NOT NULL,
    `PaidAmountRC_Budget` DECIMAL(38,6) NOT NULL,
    `PaidAmountGC_Budget` DECIMAL(38,6) NOT NULL,
    `OpenAmountTC` DECIMAL(38,6),
    `OpenAmountAC` DECIMAL(38,6),
    `OpenAmountRC` DECIMAL(38,6) NOT NULL,
    `OpenAmountGC` DECIMAL(38,6) NOT NULL,
    `OpenAmountAC_Budget` DECIMAL(38,6) NOT NULL,
    `OpenAmountRC_Budget` DECIMAL(38,6) NOT NULL,
    `OpenAmountGC_Budget` DECIMAL(38,6) NOT NULL,
    `AppliedExchangeRateTC` DECIMAL(38,6),
    `AppliedExchangeRateRC` DECIMAL(38,17) NOT NULL,
    `AppliedExchangeRateAC` DECIMAL(38,17) NOT NULL,
    `AppliedExchangeRateGC` DECIMAL(38,17) NOT NULL,
    `AppliedExchangeRateRC_Budget` DECIMAL(38,17) NOT NULL,
    `AppliedExchangeRateAC_Budget` DECIMAL(38,17) NOT NULL,
    `AppliedExchangeRateGC_Budget` DECIMAL(38,17) NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
