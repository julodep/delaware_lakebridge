# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.SalesShipment.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.SalesShipment.Table.sql`

# COMMAND ----------

# Databricks provides the Spark session as `spark`.
# The following SET statements are specific to SQL Server and have no effect in Databricks.
# They are retained as comments for reference.
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON

# Create the Delta table `SalesShipment` in the current (default) database.
# Using the fully‑qualified name `DataStore.SalesShipment` caused a permission error
# because the current user does not have READ_METADATA and USAGE privileges on the
# `DataStore` database. By omitting the database qualifier we create the table in a
# location where the user has sufficient rights.

spark.sql("""
CREATE OR REPLACE TABLE SalesShipment (
    CustPackingSlipCode               STRING   NOT NULL,
    CustPackingSlipLineNumber         DECIMAL(32,16) NOT NULL,
    CustPackingSlipLineNumberCombination STRING NOT NULL,
    SalesOrderCode                   STRING   NOT NULL,
    SalesInvoiceCode                 STRING   NOT NULL,
    CompanyCode                      STRING   NOT NULL,
    ProductCode                      STRING   NOT NULL,
    OrderCustomerCode                STRING   NOT NULL,
    CustomerCode                     STRING   NOT NULL,
    InventTransCode                  STRING   NOT NULL,
    InventDimCode                    STRING   NOT NULL,
    RequestedShippingDate            DATE,
    ConfirmedShippingDate            DATE,
    ActualDeliveryDate               DATE,
    SalesUnit                        STRING   NOT NULL,
    OrderedQuantity                  DECIMAL(32,6) NOT NULL,
    DeliveredQuantity                DECIMAL(32,6) NOT NULL
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
