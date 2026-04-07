# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.CaseActivity.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.CaseActivity.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Databricks notebook – Create `CaseActivity` Delta table
# ------------------------------------------------------------

# Retrieve catalog and schema names from widgets (or use defaults)
catalog = dbutils.widgets.get("catalog") if "catalog" in dbutils.widgets.getKeys() else "default_catalog"
schema  = dbutils.widgets.get("schema")  if "schema"  in dbutils.widgets.getKeys() else "default_schema"

# COMMAND ----------

# Build the DDL string with fully‑qualified table name
create_table_ddl = f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`CaseActivity` (
    -- Primary key‑like columns (not enforced in Delta but declared as NOT NULL)
    CaseCode            STRING  NOT NULL,
    ActivityNumber      STRING  NOT NULL,

    -- Date/time columns
    StartDateTime       DATE,
    EndDateTime         DATE,
    ActualEndDateTime   DATE,

    -- Optional identificators / descriptors
    CompanyCode         STRING,
    ActivityTimeType    STRING,
    ActivityTaskTimeType STRING,

    -- Numeric column with high precision
    ActualWork          DECIMAL(32,6) NOT NULL,

    -- Miscellaneous optional attributes
    AllDay              STRING,
    Category            STRING,
    Closed              STRING,
    DoneByWorker        STRING  NOT NULL,
    PercentageCompleted DECIMAL(32,6) NOT NULL,
    Purpose             STRING  NOT NULL,
    ResponsibleWorker   STRING  NOT NULL,
    Status              STRING,
    TypeCode            STRING  NOT NULL,
    UserMemo            STRING  NOT NULL
)
USING DELTA
"""

# COMMAND ----------

# Execute the DDL
spark.sql(create_table_ddl)

# COMMAND ----------

# Optional: verify that the table was created correctly
display(spark.sql(f"DESCRIBE FORMATTED `dbe_dbx_internships`.`datastore`.`CaseActivity`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1036)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`CaseActivity` (     -- Primary key‑like columns (not enforced in Delta but declared as NOT NULL)     CaseCode            STRING  NOT NULL,     ActivityNumber      STRING  NOT NULL,      -- Date/time columns     StartDateTime       DATE,     EndDateTime         DATE,     ActualEndDateTime   DATE,      -- Optional identificators / descriptors     CompanyCode         STRING,     ActivityTimeType    STRING,     ActivityTaskTimeType STRING,      -- Numeric column with high precision     ActualWork          DECIMAL(32,6) NOT NULL,      -- Miscellaneous optional attributes     AllDay              STRING,     Category            STRING,     Closed              STRING,     DoneByWorker        STRING  NOT NULL,     PercentageCompleted DECIMAL(32,6) NOT NULL,     Purpose             STRING  NOT NULL,     ResponsibleWorker   STRING  NOT NULL,     Status              STRING,     TypeCode            STRING  NOT NULL,     UserMemo            STRING  NOT NULL ) USING DELTA
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
