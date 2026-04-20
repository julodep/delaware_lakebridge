# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIForecastSupplyForecastStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIForecastSupplyForecastStaging.Table.sql`

# COMMAND ----------

# Create the staging table (Delta Lake format) with column names that
# do not clash with reserved SQL keywords.

spark.sql(f"""
-- 1️⃣ Create the staging table (Delta Lake format).  
-- Table name: dbe_dbx_internships.dbo.SMRBIForecastSupplyForecastStaging
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIForecastSupplyForecastStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    AMOUNT DECIMAL(32,6) NOT NULL,
    CURRENCY STRING NOT NULL,
    COMPANY STRING NOT NULL,
    ENDDATE TIMESTAMP NOT NULL,
    INVENTDIMID STRING NOT NULL,
    ITEMID STRING NOT NULL,
    MODELID STRING NOT NULL,
    PURCHPRICE DECIMAL(32,6) NOT NULL,
    PURCHQTY DECIMAL(32,6) NOT NULL,
    PURCHUNITID STRING NOT NULL,
    STARTDATE TIMESTAMP NOT NULL,
    VENDACCOUNTID STRING NOT NULL,
    FORECASTSUPPLYFORECASTDIMENSION BIGINT NOT NULL,
    PARTITION_COL STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
""")

# COMMAND ----------

print("Table created successfully: "
      f"dbe_dbx_internships.dbo.SMRBIForecastSupplyForecastStaging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 954)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN -- 1️⃣ Create the staging table (Delta Lake format).   -- Table name: _placeholder_._placeholder_.SMRBIForecastSupplyForecastStaging CREATE TABLE IF NOT EXISTS `_placeholder_`.`_placeholder_`.`SMRBIForecastSupplyForecastStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     AMOUNT DECIMAL(32,6) NOT NULL,     CURRENCY STRING NOT NULL,     COMPANY STRING NOT NULL,     ENDDATE TIMESTAMP NOT NULL,     INVENTDIMID STRING NOT NULL,     ITEMID STRING NOT NULL,     MODELID STRING NOT NULL,     PURCHPRICE DECIMAL(32,6) NOT NULL,     PURCHQTY DECIMAL(32,6) NOT NULL,     PURCHUNITID STRING NOT NULL,     STARTDATE TIMESTAMP NOT NULL,     VENDACCOUNTID STRING NOT NULL,     FORECASTSUPPLYFORECASTDIMENSION BIGINT NOT NULL,     PARTITION_COL STRING NOT NULL,     DATAAREAID STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID BIGINT NOT NULL )
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
