# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendRFQTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendRFQTransStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Databricks notebook – create the staging table `SMRBIVendRFQTransStaging`
# --------------------------------------------------------------
# NOTE:
#   * All object references are fully‑qualified: dbe_dbx_internships.dbo.SMRBIVendRFQTransStaging
#   * SQL Server data types are mapped to Spark SQL / Delta Lake types
#   * SQL Server‑specific clauses (CLUSTERED, ON [PRIMARY], PK constraints) are omitted
#   * If a primary‑key or clustering strategy is required, enforce it via Delta Lake
#      constraints or externally (e.g., data quality checks).
# --------------------------------------------------------------

# Replace `dbe_dbx_internships` and `dbo` with the actual catalog and schema names.
# They can be supplied as notebook widgets, environment variables or hard‑coded.
# Example:
#   catalog = dbutils.widgets.get("catalog")
#   schema  = dbutils.widgets.get("schema")

catalog = "dbe_dbx_internships"
schema  = "dbo"

# COMMAND ----------

# Create (or replace) the staging table in Delta format
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendRFQTransStaging` (
    -- Primary key fields from the SQL Server definition
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    INTERNALRFQID STRING NOT NULL,
    ITEMID STRING NOT NULL,
    LINEAMOUNT DECIMAL(32,6) NOT NULL,
    PURCHPRICE DECIMAL(32,6) NOT NULL,
    PURCHQTY DECIMAL(32,6) NOT NULL,
    RFQDATE TIMESTAMP NOT NULL,
    RFQID STRING NOT NULL,
    STATUS INT NOT NULL,
    COMPANY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# --------------------------------------------------------------
# Optional: Inspect the schema to confirm the mapping
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIVendRFQTransStaging`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 704)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `catalog`.`schema`.`SMRBIVendRFQTransStaging` (     -- Primary key fields from the SQL Server definition     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     INTERNALRFQID STRING NOT NULL,     ITEMID STRING NOT NULL,     LINEAMOUNT DECIMAL(32,6) NOT NULL,     PURCHPRICE DECIMAL(32,6) NOT NULL,     PURCHQTY DECIMAL(32,6) NOT NULL,     RFQDATE TIMESTAMP NOT NULL,     RFQID STRING NOT NULL,     STATUS INT NOT NULL,     COMPANY STRING NOT NULL,     PARTITION STRING NOT NULL,     DATAAREAID STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID BIGINT NOT NULL ) USING DELTA
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
