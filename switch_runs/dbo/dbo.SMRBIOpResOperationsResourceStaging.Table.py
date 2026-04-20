# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIOpResOperationsResourceStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIOpResOperationsResourceStaging.Table.sql`

# COMMAND ----------

# Define catalog and schema if not already set
catalog = "my_catalog"   # replace with your catalog name
schema  = "my_schema"    # replace with your schema name

# COMMAND ----------

# Create or replace the staging table in Delta Lake
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIOpResOperationsResourceStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    EFFICIENCYPERCENTAGE DECIMAL(32,6) NOT NULL,
    HASFINITESCHEDULINGCAPACITY INT NOT NULL,
    OPERATIONSRESOURCETYPE INT NOT NULL,
    ISINDIVIDUALRESOURCE INT NOT NULL,
    COMPANY         STRING NOT NULL,
    ROUTEGROUPID     STRING NOT NULL,
    RESOURCEID      STRING NOT NULL,
    RESOURCENAME    STRING NOT NULL,
    PARTITION       STRING NOT NULL,
    DATAAREAID      STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
    -- Composite primary key: (EXECUTIONID, RESOURCEID, DATAAREAID, PARTITION)
    -- Delta Lake does not enforce primary keys or clustering, but the
    -- definition is kept as a comment for reference.
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 879)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `my_catalog`.`my_schema`.`SMRBIOpResOperationsResourceStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID   STRING NOT NULL,     ISSELECTED    INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     EFFICIENCYPERCENTAGE DECIMAL(32,6) NOT NULL,     HASFINITESCHEDULINGCAPACITY INT NOT NULL,     OPERATIONSRESOURCETYPE INT NOT NULL,     ISINDIVIDUALRESOURCE INT NOT NULL,     COMPANY         STRING NOT NULL,     ROUTEGROUPID     STRING NOT NULL,     RESOURCEID      STRING NOT NULL,     RESOURCENAME    STRING NOT NULL,     PARTITION       STRING NOT NULL,     DATAAREAID      STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL     -- Composite primary key: (EXECUTIONID, RESOURCEID, DATAAREAID, PARTITION)     -- Delta Lake does not enforce primary keys or clustering, but the     -- definition is kept as a comment for reference. );
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
