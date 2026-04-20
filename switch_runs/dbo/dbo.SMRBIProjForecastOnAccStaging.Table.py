# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjForecastOnAccStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjForecastOnAccStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table for SMRBI project forecasts.
#
# Replacements:
#   dbe_dbx_internships  – the Databricks catalog name (use the fully‑qualified syntax)
#   dbo   – the schema (usually a database) within that catalog
#
# Notes on translation:
#   • NVARCHAR → STRING
#   • INT  → INT
#   • NUMERIC(p,s) → DECIMAL(p,s)
#   • DATETIME → TIMESTAMP
#   • Primary key constraints are not supported in Delta Lake.
#     They are commented out below for reference.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastOnAccStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    SALESCURRENCY  STRING NOT NULL,
    FORECASTMODEL  STRING NOT NULL,
    PROJECTID      STRING NOT NULL,
    QUANTITY       DECIMAL(32,6) NOT NULL,
    SALESPRICE     DECIMAL(32,6) NOT NULL,
    PROJECTDATE    TIMESTAMP NOT NULL,
    TRANSACTIONID  STRING NOT NULL,
    DESCRIPTION    STRING NOT NULL,
    COMPANY        STRING NOT NULL,
    PARTITION      STRING NOT NULL,
    DATAAREAID     STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    -- The following PRIMARY KEY definition is retained as a comment
    -- because Delta Lake does not enforce primary keys.  It may be
    -- used by downstream processes for documentation or replicated
    -- into metadata management systems.
    -- PRIMARY KEY (EXECUTIONID, TRANSACTIONID, DATAAREAID, PARTITION)
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1011)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIProjForecastOnAccStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID    STRING NOT NULL,     ISSELECTED     INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     SALESCURRENCY  STRING NOT NULL,     FORECASTMODEL  STRING NOT NULL,     PROJECTID      STRING NOT NULL,     QUANTITY       DECIMAL(32,6) NOT NULL,     SALESPRICE     DECIMAL(32,6) NOT NULL,     PROJECTDATE    TIMESTAMP NOT NULL,     TRANSACTIONID  STRING NOT NULL,     DESCRIPTION    STRING NOT NULL,     COMPANY        STRING NOT NULL,     PARTITION      STRING NOT NULL,     DATAAREAID     STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     -- The following PRIMARY KEY definition is retained as a comment     -- because Delta Lake does not enforce primary keys.  It may be     -- used by downstream processes for documentation or replicated     -- into metadata management systems.     -- PRIMARY KEY (EXECUTIONID, TRANSACTIONID, DATAAREAID, PARTITION) );
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
