# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.InventoryMovements.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.InventoryMovements.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
# Specify the target Databricks catalog and schema names.
# Replace these placeholder values with the actual catalog and schema when running the notebook.
# --------------------------------------------------------------------------
catalog = "your_catalog"
schema  = "your_schema"

# COMMAND ----------

# --------------------------------------------------------------------------
# Create the InventoryMovements table in the target catalog & schema.
# --------------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`InventoryMovements` (
    TransRecId    BIGINT,                                                -- Primary key surrogate in T‑SQL
    CompanyCode   STRING NOT NULL,                                       -- NVARCHAR(4) → STRING
    Currency      STRING NOT NULL,                                       -- NVARCHAR(3) → STRING
    CostPhysical  DECIMAL(38,6),                                         -- NUMERIC(38,6) → DECIMAL
    CostFinancial DECIMAL(38,6),                                         -- NUMERIC(38,6) → DECIMAL
    CostAdjustment DECIMAL(38,6)                                         -- NUMERIC(38,6) → DECIMAL
)
""")

# COMMAND ----------

# --------------------------------------------------------------------------
# Verify that the table schema was created as expected
# --------------------------------------------------------------------------
df = spark.sql(f"DESCRIBE `dbe_dbx_internships`.`datastore`.`InventoryMovements`")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 686)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `your_catalog`.`your_schema`.`InventoryMovements` (     TransRecId    BIGINT,                                                -- Primary key surrogate in T‑SQL     CompanyCode   STRING NOT NULL,                                       -- NVARCHAR(4) → STRING     Currency      STRING NOT NULL,                                       -- NVARCHAR(3) → STRING     CostPhysical  DECIMAL(38,6),                                         -- NUMERIC(38,6) → DECIMAL     CostFinancial DECIMAL(38,6),                                         -- NUMERIC(38,6) → DECIMAL     CostAdjustment DECIMAL(38,6)                                         -- NUMERIC(38,6) → DECIMAL )
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
