# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Operations.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Operations.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
#  Databricks notebook – Create table Operations
# ------------------------------------------------------------
# Ensure the catalog and schema variables are already defined:
#   catalog = "<your_catalog>"
#   schema  = "<your_schema>"

# ------------------------------------------------------------
#  1️⃣  Create the table
# ------------------------------------------------------------
spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Operations` (
        OperationCode      STRING NOT NULL,
        OperationName      STRING NOT NULL,
        OperationSequence  LONG,
        OperationNumber    STRING NOT NULL,
        OperationNumberNext INT   NOT NULL,
        CompanyCode        STRING NOT NULL,
        OperationPriority  INT   NOT NULL
    )
    """
)

# COMMAND ----------

# ------------------------------------------------------------
#  2️⃣  Verify that the table exists
# ------------------------------------------------------------
spark.sql(
    f"""
    SHOW TABLES IN `dbe_dbx_internships`.`datastore` LIKE 'Operations'
    """
).show(truncate=False)

# COMMAND ----------

# ------------------------------------------------------------
#  3️⃣  Optional: add a small test insert to confirm the schema
# ------------------------------------------------------------
spark.sql(
    f"""
    INSERT INTO `dbe_dbx_internships`.`datastore`.`Operations`
    VALUES (
        'OP001',      -- OperationCode
        'Setup',      -- OperationName
        NULL,         -- OperationSequence
        '1001',       -- OperationNumber
        1,            -- OperationNumberNext
        'COMP',       -- CompanyCode
        5             -- OperationPriority
    )
    """
)

# COMMAND ----------

# ------------------------------------------------------------
#  4️⃣  Query the table to ensure the insert worked
# ------------------------------------------------------------
spark.sql(
    f"""
    SELECT * FROM `dbe_dbx_internships`.`datastore`.`Operations`
    """
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 370)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN INSERT INTO `_placeholder_`.`_placeholder_`.`Operations`     VALUES (         'OP001',      -- OperationCode         'Setup',      -- OperationName         NULL,         -- OperationSequence         '1001',       -- OperationNumber         1,            -- OperationNumberNext         'COMP',       -- CompanyCode         5             -- OperationPriority     )
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
