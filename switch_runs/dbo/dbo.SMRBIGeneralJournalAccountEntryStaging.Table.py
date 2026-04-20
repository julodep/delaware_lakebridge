# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIGeneralJournalAccountEntryStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIGeneralJournalAccountEntryStaging.Table.sql`

# COMMAND ----------

# ----------------------------------------------------
# Create a staging table in the specified catalog & schema
# ----------------------------------------------------
# This is a direct translation of the T‑SQL CREATE TABLE statement.
# Spark (Delta Lake) does not support PRIMARY KEY constraints,
# so the composite key declared in the original definition is
# omitted.  You can enforce uniqueness in downstream processing
# if needed (e.g., via unique index in a relational DB or
# manual logic in PySpark).
#
# All object references are fully‑qualified: dbe_dbx_internships.dbo.{table}
# ----------------------------------------------------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIGeneralJournalAccountEntryStaging` (
        DEFINITIONGROUP STRING    NOT NULL,
        EXECUTIONID    STRING    NOT NULL,
        ISSELECTED      INT      NOT NULL,
        TRANSFERSTATUS  INT      NOT NULL,
        ACCOUNTINGDATE   TIMESTAMP NOT NULL,
        ACCOUNTINGCURRENCYAMOUNT DECIMAL(32,6) NOT NULL,
        DESCRIPTION     STRING    NOT NULL,
        DOCUMENTDATE    TIMESTAMP NOT NULL,
        DOCUMENTNUMBER  STRING    NOT NULL,
        GENERALJOURNALACCOUNTENTRYRECID BIGINT  NOT NULL,
        LEDGERNAME      STRING    NOT NULL,
        REPORTINGCURRENCYAMOUNT  DECIMAL(32,6) NOT NULL,
        TRANSACTIONCURRENCYAMOUNT DECIMAL(32,6) NOT NULL,
        TRANSACTIONCURRENCYCODE   STRING    NOT NULL,
        LEDGERDIMENSIONDISPLAYVALUE STRING NOT NULL,
        JOURNALNUMBER    STRING    NOT NULL,
        VOUCHER          STRING    NOT NULL,
        JOURNALCATEGORY  INT      NOT NULL,
        ISCREDIT         INT      NOT NULL,
        GENERALJOURNALACCOUNTENTRYDIMENSION BIGINT NOT NULL,
        PARTITION        STRING    NOT NULL,
        YSLEVENDORVALUE STRING    NOT NULL,
        YSLESHIPMENTCONTRACTVALUE STRING NOT NULL,
        YSLEINTERCOMPANYVALUE STRING NOT NULL,
        YSLELOCATIONVALUE STRING NOT NULL,
        YSLEENDCUSTOMERVALUE STRING NOT NULL,
        YSLEPRODUCTVALUE STRING NOT NULL,
        YSLEDEPARTMENTVALUE STRING NOT NULL,
        YSLELOCALACCOUNTVALUE STRING NOT NULL,
        YSLETEXT          STRING    NOT NULL,
        YSLEMAINACCOUNT  STRING    NOT NULL,
        DATAAREAID        STRING    NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL,
        RECID             BIGINT    NOT NULL
        -- Primary Key (COMPOSITE) is omitted – Delta Lake does not support it natively
    );
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1809)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIGeneralJournalAccountEntryStaging` (         DEFINITIONGROUP STRING    NOT NULL,         EXECUTIONID    STRING    NOT NULL,         ISSELECTED      INT      NOT NULL,         TRANSFERSTATUS  INT      NOT NULL,         ACCOUNTINGDATE   TIMESTAMP NOT NULL,         ACCOUNTINGCURRENCYAMOUNT DECIMAL(32,6) NOT NULL,         DESCRIPTION     STRING    NOT NULL,         DOCUMENTDATE    TIMESTAMP NOT NULL,         DOCUMENTNUMBER  STRING    NOT NULL,         GENERALJOURNALACCOUNTENTRYRECID BIGINT  NOT NULL,         LEDGERNAME      STRING    NOT NULL,         REPORTINGCURRENCYAMOUNT  DECIMAL(32,6) NOT NULL,         TRANSACTIONCURRENCYAMOUNT DECIMAL(32,6) NOT NULL,         TRANSACTIONCURRENCYCODE   STRING    NOT NULL,         LEDGERDIMENSIONDISPLAYVALUE STRING NOT NULL,         JOURNALNUMBER    STRING    NOT NULL,         VOUCHER          STRING    NOT NULL,         JOURNALCATEGORY  INT      NOT NULL,         ISCREDIT         INT      NOT NULL,         GENERALJOURNALACCOUNTENTRYDIMENSION BIGINT NOT NULL,         PARTITION        STRING    NOT NULL,         YSLEVENDORVALUE STRING    NOT NULL,         YSLESHIPMENTCONTRACTVALUE STRING NOT NULL,         YSLEINTERCOMPANYVALUE STRING NOT NULL,         YSLELOCATIONVALUE STRING NOT NULL,         YSLEENDCUSTOMERVALUE STRING NOT NULL,         YSLEPRODUCTVALUE STRING NOT NULL,         YSLEDEPARTMENTVALUE STRING NOT NULL,         YSLELOCALACCOUNTVALUE STRING NOT NULL,         YSLETEXT          STRING    NOT NULL,         YSLEMAINACCOUNT  STRING    NOT NULL,         DATAAREAID        STRING    NOT NULL,         SYNCSTARTDATETIME TIMESTAMP NOT NULL,         RECID             BIGINT    NOT NULL         -- Primary Key (COMPOSITE) is omitted – Delta Lake does not support it natively     );
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
