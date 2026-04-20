# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIRouteVersionStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIRouteVersionStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# 1️⃣ Create the Delta table that mirrors the T‑SQL
# -------------------------------------------------------------
# The original T‑SQL defined a table with many columns, all NOT NULL, and a
# complex composite primary key.  Delta Lake (the backing format for
# Spark SQL tables) does **not** enforce primary‑key constraints, so we
# simply create the schema and let the application layer do any
# uniqueness logic that is required.
#
# Data‑type mapping (T‑SQL → Spark SQL):
#   NVARCHAR           → STRING
#   INT                → INT
#   BIGINT             → BIGINT
#   DATETIME           → TIMESTAMP
#   NUMERIC(32,6)      → DECIMAL(32,6)
#
# All columns are marked as required (no NULLs) just like in the
# original T‑SQL definition.  We leave the table as a Delta table
# so that we can leverage Spark SQL features (ACID, schema
# evolution, etc.).  The primary‑key definition is added as a
# comment for reference only.
# -------------------------------------------------------------

# Define catalog and schema so that the table name can be constructed
catalog = "default"          # replace with your catalog name
schema  = "staging"          # replace with your schema name

# COMMAND ----------

sql = f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIRouteVersionStaging` (
    DEFINITIONGROUP        STRING   NOT NULL,
    EXECUTIONID            STRING   NOT NULL,
    ISSELECTED             INT      NOT NULL,
    TRANSFERSTATUS         INT      NOT NULL,
    ITEMNUMBER             STRING   NOT NULL,
    ROUTEID                STRING   NOT NULL,
    VALIDFROMDATE          TIMESTAMP NOT NULL,
    VALIDTODATE            TIMESTAMP NOT NULL,
    COMPANY                STRING   NOT NULL,
    VALIDFROMQUANTITY      DECIMAL(32,6) NOT NULL,
    `PARTITION`            STRING   NOT NULL,   -- column name `PARTITION` is quoted
    DATAAREAID             STRING   NOT NULL,
    SYNCSTARTDATETIME      TIMESTAMP NOT NULL,
    RECID                  BIGINT   NOT NULL
)
USING DELTA
COMMENT ''   -- Place for optional metadata
"""

# COMMAND ----------

# Execute the statement
spark.sql(sql)

# COMMAND ----------

# -------------------------------------------------------------
# 2️⃣ Optional: Verify creation / show the schema
# -------------------------------------------------------------
spark.table(f"dbe_dbx_internships.dbo.SMRBIRouteVersionStaging").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 828)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `default`.`staging`.`SMRBIRouteVersionStaging` (     DEFINITIONGROUP        STRING   NOT NULL,     EXECUTIONID            STRING   NOT NULL,     ISSELECTED             INT      NOT NULL,     TRANSFERSTATUS         INT      NOT NULL,     ITEMNUMBER             STRING   NOT NULL,     ROUTEID                STRING   NOT NULL,     VALIDFROMDATE          TIMESTAMP NOT NULL,     VALIDTODATE            TIMESTAMP NOT NULL,     COMPANY                STRING   NOT NULL,     VALIDFROMQUANTITY      DECIMAL(32,6) NOT NULL,     `PARTITION`            STRING   NOT NULL,   -- column name `PARTITION` is quoted     DATAAREAID             STRING   NOT NULL,     SYNCSTARTDATETIME      TIMESTAMP NOT NULL,     RECID                  BIGINT   NOT NULL ) USING DELTA COMMENT ''   -- Place for optional metadata
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
