# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------------------
# 1️⃣  Create the staging table in the specified Databricks catalog
# ---------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    DENOMINATOR   INT NOT NULL,

    FACTOR        DECIMAL(32, 16) NOT NULL,          -- numeric(32,16)
    FROMUNITSYMBOL STRING NOT NULL,
    INNEROFFSET   DECIMAL(32, 6) NOT NULL,          -- numeric(32,6)
    NUMERATOR     INT NOT NULL,
    OUTEROFFSET   DECIMAL(32, 6) NOT NULL,          -- numeric(32,6)

    PRODUCTNUMBER STRING NOT NULL,
    ROUNDING      INT NOT NULL,
    TOUNITSYMBOL  STRING NOT NULL,
    `PARTITION`   STRING NOT NULL,

    SYNCSTARTDATETIME TIMESTAMP NOT NULL,         -- datetime
    RECID              BIGINT NOT NULL            -- bigint

    -- Primary key: (EXECUTIONID, FROMUNITSYMBOL, PRODUCTNUMBER,
    --                TOUNITSYMBOL, PARTITION) is omitted because
    --                Delta Lake does not enforce clustered PKs.
)
USING DELTA
;
""")

# COMMAND ----------

# ---------------------------------------------------------------------
# 2️⃣  Verify that the table exists (optional)
# ---------------------------------------------------------------------
table_df = spark.table(f"dbe_dbx_internships.dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging")
display(table_df.limit(5))   # Show first 5 rows to confirm creation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1034)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID   STRING NOT NULL,     ISSELECTED    INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     DENOMINATOR   INT NOT NULL,      FACTOR        DECIMAL(32, 16) NOT NULL,          -- numeric(32,16)     FROMUNITSYMBOL STRING NOT NULL,     INNEROFFSET   DECIMAL(32, 6) NOT NULL,          -- numeric(32,6)     NUMERATOR     INT NOT NULL,     OUTEROFFSET   DECIMAL(32, 6) NOT NULL,          -- numeric(32,6)      PRODUCTNUMBER STRING NOT NULL,     ROUNDING      INT NOT NULL,     TOUNITSYMBOL  STRING NOT NULL,     `PARTITION`   STRING NOT NULL,      SYNCSTARTDATETIME TIMESTAMP NOT NULL,         -- datetime     RECID              BIGINT NOT NULL            -- bigint      -- Primary key: (EXECUTIONID, FROMUNITSYMBOL, PRODUCTNUMBER,     --                TOUNITSYMBOL, PARTITION) is omitted because     --                Delta Lake does not enforce clustered PKs. ) USING DELTA ;
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
