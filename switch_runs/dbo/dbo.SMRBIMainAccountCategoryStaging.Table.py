# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIMainAccountCategoryStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIMainAccountCategoryStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the staging table in the target catalog/schema
#  NOTE: Databricks Delta Lake does not support PRIMARY KEY constraints.
#        The key definition is kept as a comment for reference.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIMainAccountCategoryStaging` (
    DEFINITIONGROUP     STRING  NOT NULL,
    EXECUTIONID         STRING  NOT NULL,
    ISSELECTED          INT     NOT NULL,
    TRANSFERSTATUS      INT     NOT NULL,
    MAINACCOUNTCATEGORY STRING  NOT NULL,
    REFERENCEID         INT     NOT NULL,
    MAINACCOUNTTYPE     INT     NOT NULL,
    DESCRIPTION         STRING  NOT NULL,
    PARTITION           STRING  NOT NULL,
    SYNCSTARTDATETIME   TIMESTAMP NOT NULL,
    RECID               BIGINT  NOT NULL
    -- PRIMARY KEY (EXECUTIONID, REFERENCEID, PARTITION)
    --   is commented out because Delta Lake does not enforce PK constraints.
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 689)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBIMainAccountCategoryStaging` (     DEFINITIONGROUP     STRING  NOT NULL,     EXECUTIONID         STRING  NOT NULL,     ISSELECTED          INT     NOT NULL,     TRANSFERSTATUS      INT     NOT NULL,     MAINACCOUNTCATEGORY STRING  NOT NULL,     REFERENCEID         INT     NOT NULL,     MAINACCOUNTTYPE     INT     NOT NULL,     DESCRIPTION         STRING  NOT NULL,     PARTITION           STRING  NOT NULL,     SYNCSTARTDATETIME   TIMESTAMP NOT NULL,     RECID               BIGINT  NOT NULL     -- PRIMARY KEY (EXECUTIONID, REFERENCEID, PARTITION)     --   is commented out because Delta Lake does not enforce PK constraints. );
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
