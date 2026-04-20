# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustomerSettlementStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustomerSettlementStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
#  1.  Create the Delta table
# --------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICustomerSettlementStaging` (
    DEFINITIONGROUP          STRING  NOT NULL,
    EXECUTIONID              STRING  NOT NULL,
    ISSELECTED               INT     NOT NULL,
    TRANSFERSTATUS           INT     NOT NULL,
    SETTLEAMOUNTMST          DECIMAL(32,6) NOT NULL,
    SETTLEAMOUNTCUR          DECIMAL(32,6) NOT NULL,
    ACCOUNTNUM               STRING  NOT NULL,
    EXCHADJUSTMENT           DECIMAL(32,6) NOT NULL,
    TRANSCOMPANY             STRING  NOT NULL,
    TRANSDATE                TIMESTAMP NOT NULL,
    TRANSRECID               BIGINT  NOT NULL,
    `PARTITION`              STRING  NOT NULL,             -- quoted to avoid keyword conflict
    SYNCSTARTDATETIME        TIMESTAMP NOT NULL,
    RECID                    BIGINT  NOT NULL
    /* PRIMARY KEY constraints are not supported in Delta Lake */
)
USING DELTA
""")   # <-- The table is now available as a Delta Lake table

# COMMAND ----------

# --------------------------------------------------------------------------
#  2.  Verify creation (optional)
# --------------------------------------------------------------------------
spark.sql(f"DESC TABLE `dbe_dbx_internships`.`dbo`.`SMRBICustomerSettlementStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 894)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBICustomerSettlementStaging` (     DEFINITIONGROUP          STRING  NOT NULL,     EXECUTIONID              STRING  NOT NULL,     ISSELECTED               INT     NOT NULL,     TRANSFERSTATUS           INT     NOT NULL,     SETTLEAMOUNTMST          DECIMAL(32,6) NOT NULL,     SETTLEAMOUNTCUR          DECIMAL(32,6) NOT NULL,     ACCOUNTNUM               STRING  NOT NULL,     EXCHADJUSTMENT           DECIMAL(32,6) NOT NULL,     TRANSCOMPANY             STRING  NOT NULL,     TRANSDATE                TIMESTAMP NOT NULL,     TRANSRECID               BIGINT  NOT NULL,     `PARTITION`              STRING  NOT NULL,             -- quoted to avoid keyword conflict     SYNCSTARTDATETIME        TIMESTAMP NOT NULL,     RECID                    BIGINT  NOT NULL     /* PRIMARY KEY constraints are not supported in Delta Lake */ ) USING DELTA
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
