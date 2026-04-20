# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore4.ProductCostBreakdownQuantityRatio.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore4/datastore4_volume/datastore4/DataStore4.ProductCostBreakdownQuantityRatio.Table.sql`

# COMMAND ----------

# This notebook creates the table `ProductCostBreakdownQuantityRatio` in the
# specified Unity Catalog namespace (`{catalog}`.{schema}).  
# All T‑SQL data types are mapped to their Spark‑SQL equivalents:
#
#   * NVARCHAR(...)  → STRING
#   * NUMERIC(p,s)   → DECIMAL(p,s)
#   * REAL           → DOUBLE
#
# The `NOT NULL` constraint is preserved for columns that were defined as
# NOT NULL in the original T‑SQL.  The table is created using the
# `CREATE OR REPLACE TABLE … USING DELTA` syntax so that the schema is
# stored persistently in Unity Catalog.

spark.sql(f"""
CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`ProductCostBreakdownQuantityRatio` (
    DataAreaId              STRING NOT NULL,
    ItemNumber              STRING,
    CalculationNr           DECIMAL(38, 0),
    PriceCalcId             STRING NOT NULL,
    T1_PriceCalcId          STRING NOT NULL,
    T2_PriceCalcId          STRING,
    T3_PriceCalcId          STRING,
    T4_PriceCalcId          STRING,
    T5_PriceCalcId          STRING,
    T6_PriceCalcId          STRING,
    QtyRatioP0              DOUBLE NOT NULL,
    QtyRatioP1              DOUBLE NOT NULL,
    QtyRatioP2              DOUBLE NOT NULL,
    QtyRatioP3              DOUBLE NOT NULL,
    QtyRatioP4              DOUBLE NOT NULL,
    QtyRatioP5              DOUBLE NOT NULL,
    TotalQtyRatio           DECIMAL(38, 17)
);
""")

# COMMAND ----------

# Verify that the table was created by listing its schema
spark.sql(f"DESCRIBE TABLE `{catalog}`.`{schema}`.`ProductCostBreakdownQuantityRatio`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
