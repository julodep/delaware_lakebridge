# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PlannedPurchaseOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.PlannedPurchaseOrder.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook: Create the PlannedPurchaseOrder table
# ------------------------------------------------------------------
# This notebook uses the fully-qualified catalog and schema names
# (e.g. `dbe_dbx_internships`.`datastore`) as specified in the problem statement.
# All string data is mapped to Spark SQL STRING, datetime values to TIMESTAMP,
# and precision-numeric values to DECIMAL(32,6).
# ------------------------------------------------------------------


# ------------------------------------------------------------
# 1. Create the PlannedPurchaseOrder Delta table
# ------------------------------------------------------------
# The columns are defined exactly as in the T-SQL table, with the
# appropriate Spark SQL data types.
# The NOT NULL constraint is preserved by specifying `NOT NULL`
# after the column type.  Spark Delta will enforce nullability.
# ------------------------------------------------------------

spark.sql("""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.PlannedPurchaseOrder (
    PlannedPurchaseOrderCode STRING NOT NULL,
    ProductCode STRING NOT NULL,
    SupplierCode STRING NOT NULL,
    CompanyCode STRING NOT NULL,
    ProductConfigurationCode STRING NOT NULL,
    PurchaseOrderCode STRING NOT NULL,
    RequirementDate TIMESTAMP NOT NULL,
    RequestedDate TIMESTAMP NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    DeliveryDate TIMESTAMP,            -- NULL allowed
    Status STRING,                      -- NULL allowed
    LeadTime INT NOT NULL,
    InventoryUnit STRING NOT NULL,
    RequirementQuantity DECIMAL(32,6) NOT NULL,
    PurchaseUnit STRING NOT NULL,
    PurchaseQuantity DECIMAL(32,6) NOT NULL
)
USING delta  -- Ensure it's a Delta Lake table for ACID guarantees
""")


# COMMAND ----------

# ------------------------------------------------------------
# 2. Verify table creation
# ------------------------------------------------------------
# This optional step shows the table schema in the notebook output.
display(spark.sql("DESCRIBE EXTENDED `dbe_dbx_internships`.`datastore`.PlannedPurchaseOrder"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 784)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `catalog`.`schema`.PlannedPurchaseOrder (     PlannedPurchaseOrderCode STRING NOT NULL,     ProductCode STRING NOT NULL,     SupplierCode STRING NOT NULL,     CompanyCode STRING NOT NULL,     ProductConfigurationCode STRING NOT NULL,     PurchaseOrderCode STRING NOT NULL,     RequirementDate TIMESTAMP NOT NULL,     RequestedDate TIMESTAMP NOT NULL,     OrderDate TIMESTAMP NOT NULL,     DeliveryDate TIMESTAMP,            -- NULL allowed     Status STRING,                      -- NULL allowed     LeadTime INT NOT NULL,     InventoryUnit STRING NOT NULL,     RequirementQuantity DECIMAL(32,6) NOT NULL,     PurchaseUnit STRING NOT NULL,     PurchaseQuantity DECIMAL(32,6) NOT NULL ) USING delta  -- Ensure it's a Delta Lake table for ACID guarantees
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
