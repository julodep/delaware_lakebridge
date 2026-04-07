# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PurchaseBudget.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.PurchaseBudget.Table.sql`

# COMMAND ----------

import pyspark.sql
from pyspark.sql.functions import (
    col,
    date_add,
    add_months,
    concat,
    lpad,
    lit,
    current_timestamp,
    current_date,
    current_user,
    date_format,
    expr
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# 1. Temporary table: #RecentOrders  ->   RecentOrders (Delta table)
# ---------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`RecentOrders` (
    OrderID INT,
    CustomerName STRING,
    TotalAmount DECIMAL(10,2),
    OrderDate DATE
)
""")

# COMMAND ----------

spark.sql(f"""
INSERT INTO `dbe_dbx_internships`.`datastore`.`RecentOrders`
SELECT 
    o.OrderID,
    concat_ws(' ', c.FirstName, c.LastName) AS CustomerName,
    SUM(oi.Price * oi.Quantity) AS TotalAmount,
    o.OrderDate
FROM `dbe_dbx_internships`.`datastore`.Orders o
JOIN `dbe_dbx_internships`.`datastore`.Customers c ON o.CustomerID = c.CustomerID
JOIN `dbe_dbx_internships`.`datastore`.OrderItems oi ON o.OrderID = oi.OrderID
WHERE o.OrderDate > add_months(current_timestamp(), -3)
GROUP BY o.OrderID, c.FirstName, c.LastName, o.OrderDate
""")

# COMMAND ----------

top_customers = spark.sql(f"""
SELECT CustomerName, SUM(TotalAmount) AS TotalSpent
FROM `dbe_dbx_internships`.`datastore`.`RecentOrders`
GROUP BY CustomerName
ORDER BY TotalSpent DESC
LIMIT 10
""")
display(top_customers)

# COMMAND ----------

# ---------------------------------------------------------------------------
# 2. Procedure: dbo.UpdateProductPrice
# ---------------------------------------------------------------------------

dbutils.widgets.text("product_id", "")     # @ProductID
dbutils.widgets.text("new_price", "")     # @NewPrice

# COMMAND ----------

try:
    product_id = int(dbutils.widgets.get("product_id"))
    new_price  = float(dbutils.widgets.get("new_price"))
except Exception as e:
    print("Error parsing input parameters:", e)
    dbutils.notebook.exit("Invalid parameters")

# COMMAND ----------

exists_df = spark.sql(f"""
SELECT 1 FROM `dbe_dbx_internships`.`datastore`.Products
WHERE ProductID = {product_id}
""")
if exists_df.count() == 0:
    print("Product not found")
    dbutils.notebook.exit("Product missing")

# COMMAND ----------

spark.sql(f"""
UPDATE `dbe_dbx_internships`.`datastore`.Products
SET Price = {new_price}
WHERE ProductID = {product_id}
""")

# COMMAND ----------

updated_df = spark.sql(f"""
SELECT * FROM `dbe_dbx_internships`.`datastore`.Products
WHERE ProductID = {product_id}
""")
display(updated_df)

# COMMAND ----------

# ---------------------------------------------------------------------------
# 3. Procedure: dbo.UpdateProductPriceWithTransaction
# ---------------------------------------------------------------------------

dbutils.widgets.text("product_id_tx", "")
dbutils.widgets.text("new_price_tx", "")

# COMMAND ----------

try:
    product_id_tx = int(dbutils.widgets.get("product_id_tx"))
    new_price_tx  = float(dbutils.widgets.get("new_price_tx"))
except Exception as e:
    print("Error parsing input parameters:", e)
    dbutils.notebook.exit("Invalid parameters")

# COMMAND ----------

try:
    spark.sql(f"""
    UPDATE `dbe_dbx_internships`.`datastore`.Products
    SET Price = {new_price_tx}
    WHERE ProductID = {product_id_tx}
    """)
except Exception as err:
    print("Update failed – rolling back (if needed)")
    raise err

# COMMAND ----------

updated_tx_df = spark.sql(f"""
SELECT * FROM `dbe_dbx_internships`.`datastore`.Products
WHERE ProductID = {product_id_tx}
""")
display(updated_tx_df)

# COMMAND ----------

# ---------------------------------------------------------------------------
# 4. Function: myschema.fn_getallocationdata
# ---------------------------------------------------------------------------
# Spark SQL does not support table-valued functions directly.
# This logical view can be queried with filters for SnapshotStart and Sector.
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`v_getallocationdata` AS
WITH
    CurrentDGO AS (
        SELECT DISTINCT DGO
        FROM `dbe_dbx_internships`.`datastore`.`factreportuserpermissions`
        WHERE UserPrincipalName = current_user()
    ),
    DataMapped AS (
        SELECT
            F.*,
            UPPER(TRIM(
                CASE F.ODSSource
                    WHEN 'fluv'  THEN 'FLUVIUS'
                    WHEN 'sibe'  THEN 'SIBELGA'
                    WHEN 'ores'  THEN 'ORES'
                    ELSE F.ODSSource
                END
            )) AS ODSSource_Normalized
        FROM `dbe_dbx_internships`.`datastore`.`allocation`.Allocation F
    )
SELECT
    F.ODSSource,
    F.AllocationMonth,
    F.DGOName,
    F.Sector,
    CASE F.Sector
        WHEN 'Gas'        THEN 'SE PI 6'
        WHEN 'Electricity' THEN 'SE PI 5'
    END AS PIId,
    CASE WHEN F.AllocationRunId IS NULL THEN 1 ELSE 0 END AS Missing
FROM DataMapped F
JOIN CurrentDGO C ON F.DGOName = C.DGO
WHERE
    F.FirstRun = 1;
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 5. Scalar Function: ETL.fn_TimeKeyInt
# ---------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE FUNCTION `dbe_dbx_internships`.`datastore`.fn_TimeKeyInt
(
    V_Time TIMESTAMP
)
RETURNS INT
AS
    CAST(
        CONCAT(
            LPAD(CAST(EXTRACT(HOUR FROM V_Time) AS STRING), 2, '0'),
            LPAD(CAST(EXTRACT(MINUTE FROM V_Time) AS STRING), 2, '0')
        ) AS INT
    )
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 6. Function: dbo.get_orders
# ---------------------------------------------------------------------------
# Replaced with a Python helper that returns a DataFrame.
def get_orders(customer_id: int):
    return spark.sql(f"""
    SELECT
        order_id  AS OrderID,
        order_date AS OrderDate,
        total
    FROM `dbe_dbx_internships`.`datastore`.orders
    WHERE customer_id = {customer_id}
    """)

# COMMAND ----------

# ---------------------------------------------------------------------------
# 7. Table: DataStore.PurchaseBudget
# ---------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`PurchaseBudget` (
    ProductCode STRING,
    ForecastModelCode STRING,
    CompanyCode STRING,
    SupplierCode STRING,
    DefaultExchangeRateTypeCode STRING,
    BudgetExchangeRateTypeCode STRING,
    TransactionCurrencyCode STRING,
    AccountingCurrencyCode STRING,
    ReportingCurrencyCode STRING,
    GroupCurrencyCode STRING,
    DefaultDimension BIGINT,
    InventDimCode STRING,
    BudgetDate DATE,
    PurchaseUnit STRING,
    BudgetQuantity DECIMAL(32,6),
    PurchUnitPriceTC DECIMAL(32,6),
    PurchUnitPriceAC DECIMAL(38,6),
    PurchUnitPriceRC DECIMAL(38,6),
    PurchUnitPriceGC DECIMAL(38,6),
    PurchUnitPriceAC_Budget DECIMAL(38,6),
    PurchUnitPriceRC_Budget DECIMAL(38,6),
    PurchUnitPriceGC_Budget DECIMAL(38,6),
    BudgetAmountTC DECIMAL(32,6),
    BudgetAmountAC DECIMAL(38,6),
    BudgetAmountRC DECIMAL(38,6),
    BudgetAmountGC DECIMAL(38,6),
    BudgetAmountAC_Budget DECIMAL(38,6),
    BudgetAmountRC_Budget DECIMAL(38,6),
    BudgetAmountGC_Budget DECIMAL(38,6),
    AppliedExchangeRateTC DECIMAL(38,6),
    AppliedExchangeRateRC DECIMAL(38,21),
    AppliedExchangeRateAC DECIMAL(38,21),
    AppliedExchangeRateGC DECIMAL(38,21),
    AppliedExchangeRateRC_Budget DECIMAL(38,21),
    AppliedExchangeRateAC_Budget DECIMAL(38,21),
    AppliedExchangeRateGC_Budget DECIMAL(38,21)
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 8: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'CAST'. SQLSTATE: 42601 (line 1, pos 125)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE FUNCTION `_placeholder_`.`_placeholder_`.fn_TimeKeyInt (     V_Time TIMESTAMP ) RETURNS INT AS     CAST(         CONCAT(             LPAD(CAST(EXTRACT(HOUR FROM V_Time) AS STRING), 2, '0'),             LPAD(CAST(EXTRACT(MINUTE FROM V_Time) AS STRING), 2, '0')         ) AS INT     )
# MAGIC -----------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
