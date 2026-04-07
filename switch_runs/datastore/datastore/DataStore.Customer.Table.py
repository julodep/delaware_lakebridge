# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Customer.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Customer.Table.sql`

# COMMAND ----------

# Create the persistent table `Customer` in the target Unity Catalog namespace
# All column definitions are converted to Databricks / Delta Lake compatible types.
# Primary keys and indexes are omitted because Spark does not support them in this context.

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Customer` (
    CustomerId BIGINT NOT NULL,
    CompanyCode STRING,
    CustomerCode STRING NOT NULL,
    CustomerName STRING NOT NULL,
    CustomerCodeName STRING,
    CustomerGroup STRING NOT NULL,
    CustomerGroupName STRING NOT NULL,
    CustomerGroupCodeName STRING NOT NULL,
    CustomerClass STRING NOT NULL,
    CustomerClassName STRING NOT NULL,
    CustomerClassCodeName STRING NOT NULL,
    Address STRING,
    PostalCode STRING NOT NULL,
    City STRING NOT NULL,
    Country STRING NOT NULL,
    SalesGroup STRING NOT NULL,
    Agent STRING NOT NULL,
    SalesResponsibleCode STRING NOT NULL,
    SalesResponsibleName STRING NOT NULL,
    SalesSegmentCode STRING NOT NULL,
    SalesSubSegmentCode STRING NOT NULL,
    DeliveryTerms STRING NOT NULL,
    OnholdStatus STRING,
    CreditLimitIsMandatory STRING,
    CreditLimit DECIMAL(32,6) NOT NULL,
    CompanyChain STRING NOT NULL,
    TaxGroup STRING NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
