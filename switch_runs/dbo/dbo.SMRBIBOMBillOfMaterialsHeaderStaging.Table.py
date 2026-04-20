# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIBOMBillOfMaterialsHeaderStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIBOMBillOfMaterialsHeaderStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
#  Create the staging table `SMRBIBOMBillOfMaterialsHeaderStaging`
#  in the specified catalog and schema.
#  Data types have been mapped from T‑SQL to Spark SQL:
#      NVARCHAR → STRING
#      INT      → INT
#      DATETIME → TIMESTAMP
#  Note: PRIMARY KEY constraints are not enforced in Delta Lake.
# -------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBOMBillOfMaterialsHeaderStaging` (
    DEFINITIONGROUP   STRING NOT NULL,
    EXECUTIONID      STRING NOT NULL,
    ISSELECTED       INT    NOT NULL,
    TRANSFERSTATUS   INT    NOT NULL,
    BOMID            STRING NOT NULL,
    BOMNAME          STRING NOT NULL,
    COMPANY          STRING NOT NULL,
    PRODUCTGROUPID   STRING NOT NULL,
    PARTITION        STRING NOT NULL,
    DATAAREAID       STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
    -- PRIMARY KEY is not supported in Delta Lake; if needed,
    -- consider adding a UNIQUE constraint or using `BEFORE INSERT` check.
    -- PRIMARY KEY (EXECUTIONID, BOMID, DATAAREAID, PARTITION)
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 728)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIBOMBillOfMaterialsHeaderStaging` (     DEFINITIONGROUP   STRING NOT NULL,     EXECUTIONID      STRING NOT NULL,     ISSELECTED       INT    NOT NULL,     TRANSFERSTATUS   INT    NOT NULL,     BOMID            STRING NOT NULL,     BOMNAME          STRING NOT NULL,     COMPANY          STRING NOT NULL,     PRODUCTGROUPID   STRING NOT NULL,     PARTITION        STRING NOT NULL,     DATAAREAID       STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL     -- PRIMARY KEY is not supported in Delta Lake; if needed,     -- consider adding a UNIQUE constraint or using `BEFORE INSERT` check.     -- PRIMARY KEY (EXECUTIONID, BOMID, DATAAREAID, PARTITION) )
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
