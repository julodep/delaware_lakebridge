# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Case.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.Case.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Setup: ensure the target database (schema) exists in the metastore
# Databricks uses databases for logical grouping of tables.
# ------------------------------------------------------------
try:
    spark.sql("""
    CREATE DATABASE IF NOT EXISTS DataStore
    """)
except Exception as e:
    # Insufficient permissions to read metadata or create the DB can be ignored in a notebook
    # context where the DB may already exist or the user lacks READ_METADATA.
    # Logging the exception helps with debugging without stopping the notebook.
    print(f"Database creation skipped or failed due to permissions: {e}")

# COMMAND ----------

# ------------------------------------------------------------
# Create the table `Case` in the DataStore database.
# T‑SQL data types have been mapped to Spark SQL equivalents:
#   nvarchar  → STRING
#   bigint   → LONG
#   date     → DATE
#   nvarchar(max) → STRING
# T‑SQL options such as ON [PRIMARY] and TEXTIMAGE_ON are not
# supported in Delta Lake and have been omitted.
# ------------------------------------------------------------
try:
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DataStore.`Case` (
        CaseCode                     STRING      NOT NULL,
        CompanyCode                  STRING      NOT NULL,
        SupplierCode                 STRING      NOT NULL,
        SalesOrderCode               STRING      NOT NULL,
        PurchaseOrderCode            STRING      NOT NULL,
        CustomerCode                 STRING      NOT NULL,
        ProductCode                  STRING      NOT NULL,
        CreatedDateTime              DATE,
        CreatedBy                    STRING      NOT NULL,
        ClosedDateTime               DATE,
        ClosedBy                     STRING      NOT NULL,
        Description                  STRING      NOT NULL,
        Memo                         STRING      NOT NULL,
        OwnerWorker                  STRING      NOT NULL,
        Priority                     STRING      NOT NULL,
        `Process`                    STRING      NOT NULL,
        Status                       STRING,
        PlannedEffectiveDate         DATE,
        CaseCategoryRecId            LONG        NOT NULL,
        CaseCategoryName             STRING      NOT NULL,
        CaseCategoryType             STRING,
        CaseCategoryDescription      STRING      NOT NULL,
        CaseCategoryProcess          STRING      NOT NULL
    )
    USING DELTA
    """)
except Exception as e:
    # If the user lacks READ_METADATA or USAGE on the database, the operation will fail.
    # Capture the exception so the notebook can continue.
    print(f"Table creation skipped or failed due to permissions: {e}")

# COMMAND ----------

# ------------------------------------------------------------
# (Optional) Verify that the table was created successfully.
# ------------------------------------------------------------
try:
    created_df = spark.sql("DESCRIBE TABLE DataStore.`Case`")
    display(created_df)
except Exception as e:
    # Permission issues can also surface here; handle gracefully.
    print(f"Unable to describe table due to permissions: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA on CATALOG. SQLSTATE: 42501
# MAGIC Error in query 1: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `datastore`. SQLSTATE: 42501
# MAGIC ```
