# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventSumStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventSumStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Table: SMRBIInventSumStaging
# ------------------------------------------------------------------
# The original T‑SQL statement creates a normal table with a
# clustered primary-key constraint. Delta Lake (Databricks) does not
# support clustered indexes or declare primary keys in the same way as
# SQL Server. The constraint is omitted; if uniqueness must be
# enforced you can add a separate validation step after data loading.
#
# Data type mapping:
#   NVARCHAR → STRING
#   NUMERIC(p,s) → DECIMAL(p,s)
#   DATETIME → TIMESTAMP
# The NOT NULL constraints are preserved because Spark / Delta
# supports them directly, but the primary-key definition is commented
# out for clarity.
#
# Full qualification of the table: dbe_dbx_internships.dbo.SMRBIInventSumStaging

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIInventSumStaging (
    DEFINITIONGROUP   STRING     NOT NULL,
    EXECUTIONID       STRING     NOT NULL,
    ISSELECTED        INT        NOT NULL,
    TRANSFERSTATUS    INT        NOT NULL,
    INVENTDIMID       STRING     NOT NULL,
    ITEMID            STRING     NOT NULL,
    PHYSICALINVENT    DECIMAL(32,6) NOT NULL,
    PHYSICALVALUE     DECIMAL(32,6) NOT NULL,
    COMPANY           STRING     NOT NULL,
    AVAILPHYSICAL     DECIMAL(32,6) NOT NULL,
    RESERVORDERED     DECIMAL(32,6) NOT NULL,
    RESERVPHYSICAL    DECIMAL(32,6) NOT NULL,
    PARTITION         STRING     NOT NULL,
    DATAAREAID        STRING     NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP   NOT NULL
    -- PRIMARY KEY (EXECUTIONID, INVENTDIMID, ITEMID, DATAAREAID, PARTITION)
    -- Delta Lake currently does not support clustered primary keys.
)
USING DELTA;
""")

# COMMAND ----------

# Example: check for duplicate key combinations
spark.sql(f"""
SELECT EXECUTIONID, INVENTDIMID, ITEMID, DATAAREAID, PARTITION, COUNT(*)
FROM `dbe_dbx_internships`.`dbo`.SMRBIInventSumStaging
GROUP BY EXECUTIONID, INVENTDIMID, ITEMID, DATAAREAID, PARTITION
HAVING COUNT(*) > 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 897)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.SMRBIInventSumStaging (     DEFINITIONGROUP   STRING     NOT NULL,     EXECUTIONID       STRING     NOT NULL,     ISSELECTED        INT        NOT NULL,     TRANSFERSTATUS    INT        NOT NULL,     INVENTDIMID       STRING     NOT NULL,     ITEMID            STRING     NOT NULL,     PHYSICALINVENT    DECIMAL(32,6) NOT NULL,     PHYSICALVALUE     DECIMAL(32,6) NOT NULL,     COMPANY           STRING     NOT NULL,     AVAILPHYSICAL     DECIMAL(32,6) NOT NULL,     RESERVORDERED     DECIMAL(32,6) NOT NULL,     RESERVPHYSICAL    DECIMAL(32,6) NOT NULL,     PARTITION         STRING     NOT NULL,     DATAAREAID        STRING     NOT NULL,     SYNCSTARTDATETIME TIMESTAMP   NOT NULL     -- PRIMARY KEY (EXECUTIONID, INVENTDIMID, ITEMID, DATAAREAID, PARTITION)     -- Delta Lake currently does not support clustered primary keys. ) USING DELTA;
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
