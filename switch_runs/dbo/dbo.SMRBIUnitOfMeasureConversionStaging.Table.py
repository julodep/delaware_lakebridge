# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIUnitOfMeasureConversionStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIUnitOfMeasureConversionStaging.Table.sql`

# COMMAND ----------

# Script to create the staging table SMRBIUnitOfMeasureConversionStaging
# in the specified Databricks catalog and schema.

# Replace these with your actual catalog and schema names
catalog = "your_catalog"
schema  = "your_schema"

# COMMAND ----------

# Create or replace the table with proper backticks around the table name
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIUnitOfMeasureConversionStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    DENOMINATOR INT NOT NULL,
    FACTOR DECIMAL(32,16) NOT NULL,
    INNEROFFSET DECIMAL(32,6) NOT NULL,
    NUMERATOR INT NOT NULL,
    OUTEROFFSET DECIMAL(32,6) NOT NULL,
    ROUNDING INT NOT NULL,
    FROMUNITSYMBOL STRING NOT NULL,
    TOUNITSYMBOL STRING NOT NULL,
    PRODUCT BIGINT NOT NULL,
    UNITOFMEASURECONVERSIONRECID BIGINT NOT NULL,
    UOMMODIFIEDDATETIME TIMESTAMP NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
    -- PRIMARY KEY (EXECUTIONID, FROMUNITSYMBOL, TOUNITSYMBOL, PARTITION)
)
""")

# COMMAND ----------

# Verify that the table was created correctly
df = spark.sql(f"""
SHOW TABLES IN `dbe_dbx_internships`.`dbo`
LIKE 'SMRBIUnitOfMeasureConversionStaging'
""")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 801)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `your_catalog`.`your_schema`.`SMRBIUnitOfMeasureConversionStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     DENOMINATOR INT NOT NULL,     FACTOR DECIMAL(32,16) NOT NULL,     INNEROFFSET DECIMAL(32,6) NOT NULL,     NUMERATOR INT NOT NULL,     OUTEROFFSET DECIMAL(32,6) NOT NULL,     ROUNDING INT NOT NULL,     FROMUNITSYMBOL STRING NOT NULL,     TOUNITSYMBOL STRING NOT NULL,     PRODUCT BIGINT NOT NULL,     UNITOFMEASURECONVERSIONRECID BIGINT NOT NULL,     UOMMODIFIEDDATETIME TIMESTAMP NOT NULL,     PARTITION STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID BIGINT NOT NULL     -- PRIMARY KEY (EXECUTIONID, FROMUNITSYMBOL, TOUNITSYMBOL, PARTITION) )
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
