# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjInvoiceRevenueStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjInvoiceRevenueStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
#   Databricks Python script – Create table
#   Target catalog: dbe_dbx_internships
#   Target schema : dbo
#   Fully‑qualified name: dbe_dbx_internships.dbo.SMRBIProjInvoiceRevenueStaging
#
#   Note:
#     Spark SQL (and Delta Lake) does not support PRIMARY KEY constraints in the CREATE TABLE statement.
#     The primary‑key definition has been removed for compatibility; if uniqueness enforcement is required
#     it should be implemented at the application level or via Delta Lake's MERGE.
# ------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIProjInvoiceRevenueStaging (
    -- 1. Definition group
    DEFINITIONGROUP   STRING   NOT NULL,

    -- 2. Execution identifiers
    EXECUTIONID       STRING   NOT NULL,

    -- 3. Flags
    ISSELECTED        INT      NOT NULL,
    TRANSFERSTATUS    INT      NOT NULL,

    -- 4. Dimensions
    CATEGORYID        STRING   NOT NULL,
    CURRENCYID        STRING   NOT NULL,
    COMPANY           STRING   NOT NULL,

    -- 5. Dates
    INVOICEDATE       TIMESTAMP NOT NULL,
    TRANSDATE         TIMESTAMP NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,

    -- 6. Financial amounts
    LINEAMOUNT        DECIMAL(32,6) NOT NULL,
    QTY               DECIMAL(32,6) NOT NULL,
    TAXAMOUNT         DECIMAL(32,6) NOT NULL,

    -- 7. Identity columns
    PROJID            STRING   NOT NULL,
    PROJINVOICEID     STRING   NOT NULL,
    TRANSID           STRING   NOT NULL,
    TXT               STRING   NOT NULL,
    `PARTITION`       STRING   NOT NULL,
    DATAAREAID        STRING   NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1067)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.SMRBIProjInvoiceRevenueStaging (     -- 1. Definition group     DEFINITIONGROUP   STRING   NOT NULL,      -- 2. Execution identifiers     EXECUTIONID       STRING   NOT NULL,      -- 3. Flags     ISSELECTED        INT      NOT NULL,     TRANSFERSTATUS    INT      NOT NULL,      -- 4. Dimensions     CATEGORYID        STRING   NOT NULL,     CURRENCYID        STRING   NOT NULL,     COMPANY           STRING   NOT NULL,      -- 5. Dates     INVOICEDATE       TIMESTAMP NOT NULL,     TRANSDATE         TIMESTAMP NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,      -- 6. Financial amounts     LINEAMOUNT        DECIMAL(32,6) NOT NULL,     QTY               DECIMAL(32,6) NOT NULL,     TAXAMOUNT         DECIMAL(32,6) NOT NULL,      -- 7. Identity columns     PROJID            STRING   NOT NULL,     PROJINVOICEID     STRING   NOT NULL,     TRANSID           STRING   NOT NULL,     TXT               STRING   NOT NULL,     `PARTITION`       STRING   NOT NULL,     DATAAREAID        STRING   NOT NULL )
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
