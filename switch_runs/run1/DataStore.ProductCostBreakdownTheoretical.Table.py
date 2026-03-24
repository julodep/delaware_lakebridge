# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.ProductCostBreakdownTheoretical.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.ProductCostBreakdownTheoretical.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------------------
# Ensure we are using the default catalog where the user has metadata read rights.
# ----------------------------------------------------------------------
spark.sql("USE CATALOG spark_catalog")

# COMMAND ----------

# ----------------------------------------------------------------------
# Create the target database (use lower‑case name to match the permission
# error reference). If the database already exists, this is a no‑op.
# ----------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS datastore")

# COMMAND ----------

# ----------------------------------------------------------------------
# Create the Delta table `ProductCostBreakdownTheoretical` in the `datastore`
# schema.  All T‑SQL‐specific options are omitted because they are not
# supported in Databricks.  The table is created as a Delta Lake table.
# ----------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS datastore.ProductCostBreakdownTheoretical (
    BOM                     INT,
    BOMCalcTransRecId       LONG,
    CalcGroupCode           STRING,
    CalcType                INT,
    ConsistOfPrice          STRING,
    ConsumptionConstant     DECIMAL(32,16),
    ConsumptionVariable     DECIMAL(32,16),
    ConsumpType             INT,
    CostCalculationMethod   INT,
    CostGroupCode           STRING,
    CostMarkup              DECIMAL(32,16),
    CostMarkupQty           DECIMAL(32,16),
    CostPrice               DECIMAL(32,16),
    CostPriceModelUsed      INT,
    CostPriceQty            DECIMAL(32,16),
    CostPriceUnit           DECIMAL(32,12),
    DataAreaId              STRING,
    InventDimCode           STRING,
    Level_                  INT,
    LineNum                 DECIMAL(32,16),
    NetWeightQty            DECIMAL(32,12),
    NumOfSeries             DECIMAL(32,6),
    OprId                   STRING,
    OprNum                  INT,
    OprNumNext              INT,
    OprPriority             INT,
    ParentBOMCalcTrans      LONG,
    PriceCalcId             STRING,
    Qty                     DECIMAL(32,6),
    Resource_               STRING,
    SalesMarkup             DECIMAL(32,6),
    SalesMarkupQty          DECIMAL(32,6),
    SalesPrice              DECIMAL(32,6),
    SalesPriceQty           DECIMAL(32,6),
    SalesPriceUnit          DECIMAL(32,12),
    TransDate               TIMESTAMP,
    UnitCode                STRING
)
USING DELTA
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
