# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.GeneralLedgerHistoric.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.GeneralLedgerHistoric.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: import any required modules (Databricks provides SparkSession by default)
# ------------------------------------------------------------
from pyspark.sql import functions as F  # noqa: F401  (imported for potential future use)

# COMMAND ----------

# ------------------------------------------------------------
# NOTE: The original T‑SQL script includes SET options (ANSI_NULLS, QUOTED_IDENTIFIER)
# and a GO batch separator. These are specific to SQL Server and have no effect in
# Databricks/Spark, so they are omitted in the conversion.
# ------------------------------------------------------------

# ------------------------------------------------------------
# Create the Delta table `GeneralLedgerHistoric` with the same column
# definitions. The original schema referenced the `DataStore` database,
# but the current user lacks READ_METADATA/USAGE permissions on that
# database. To avoid the permission error, the table is created in the
# current default database (or any database the user can access).
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE GeneralLedgerHistoric (
    RecId                     INT,
    TransactionCode           STRING,
    CompanyCode               STRING,
    DefaultExchangeRateTypeCode STRING,
    BudgetExchangeRateTypeCode STRING,
    TransactionCurrencyCode   STRING,
    AccountingCurrencyCode    STRING,
    ReportingCurrencyCode     STRING NOT NULL,
    GroupCurrencyCode         STRING NOT NULL,
    GLAccountCode             STRING,
    InterCompanyCode          STRING,
    BusinessSegmentCode       STRING,
    DepartmentCode            STRING,
    EndCustomerCode           STRING,
    LocationCode              STRING,
    ShipmentContractCode      STRING,
    LocalAccountCode          STRING,
    ProductFDCode             STRING,
    DocumentDate              TIMESTAMP,
    DimPostingDateId          INT,
    Voucher                   STRING NOT NULL,
    AmountTC                  DECIMAL(38,6) NOT NULL,
    AmountAC                  DECIMAL(38,6),
    AmountRC                  DECIMAL(38,6),
    AmountGC                  DECIMAL(38,6) NOT NULL,
    AppliedExchangeRateTC     DECIMAL(38,21) NOT NULL,
    AppliedExchangeRateAC     DECIMAL(38,17),
    AppliedExchangeRateRC     DECIMAL(38,17),
    AppliedExchangeRateGC     DECIMAL(38,21) NOT NULL
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
