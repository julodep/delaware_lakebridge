# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDeliveryTermsStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDeliveryTermsStaging.Table.sql`

# COMMAND ----------

# Create the SMRBIDeliveryTermsStaging table in the target catalog/scheme
# The statement is translated from the original T-SQL:
#   - NVARCHAR → STRING
#   - DATETIME → TIMESTAMP
#   - INT remains INT
#   - PRIMARY KEY and other index hints are not supported in Delta Lake
#
# Note: Replace dbe_dbx_internships and dbo with your actual catalog and schema names
# or use Databricks widgets to set them dynamically.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.SMRBIDeliveryTermsStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    TERMSCODE STRING NOT NULL,
    TERMSDESCRIPTION STRING NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    -- PRIMARY KEY constraint is not supported in Databricks SQL/Dynamo;
    -- we keep it commented to document the original intent.
    -- PRIMARY KEY (EXECUTIONID, TERMSCODE, DATAAREAID, PARTITION)
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 632)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `_placeholder_`.`_placeholder_`.SMRBIDeliveryTermsStaging (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     TERMSCODE STRING NOT NULL,     TERMSDESCRIPTION STRING NOT NULL,     COMPANY STRING NOT NULL,     PARTITION STRING NOT NULL,     DATAAREAID STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     -- PRIMARY KEY constraint is not supported in Databricks SQL/Dynamo;     -- we keep it commented to document the original intent.     -- PRIMARY KEY (EXECUTIONID, TERMSCODE, DATAAREAID, PARTITION) )
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
