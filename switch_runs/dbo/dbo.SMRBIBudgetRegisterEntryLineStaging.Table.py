# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIBudgetRegisterEntryLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIBudgetRegisterEntryLineStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------------------------------
# 1. Create the staging table in the target catalog and schema
# -------------------------------------------------------------------------------------------------
#   • All objects are fully‑qualified:  `dbe_dbx_internships`.`dbo`.`SMRBIBudgetRegisterEntryLineStaging`
#   • Data‑type mapping has been applied:
#         NVARCHAR → STRING
#         INT      → INT
#         BIGINT   → BIGINT
#         NUMERIC(p,s) → DECIMAL(p,s)
#         DATETIME → TIMESTAMP
#   • The original T‑SQL `PRIMARY KEY CLUSTERED` clause is **not** supported by Databricks Delta.
#     Instead, you can:
#       * Add a unique index manually if desired
#       * Enforce uniqueness in downstream logic or by using Delta Lake constraints (CHECK)
#     For now, the constraint is omitted and a comment is added for reference.
# -------------------------------------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIBudgetRegisterEntryLineStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID    STRING NOT NULL,
    ISSELECTED     INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,

    ACCOUNTINGCURRENCYAMOUNT DECIMAL(32,6) NOT NULL,
    AMOUNTTYPE              INT          NOT NULL,

    BUDGETTRANSACTIONHEADERRECID BIGINT NOT NULL,
    CURRENCYCODE               STRING NOT NULL,

    ENTRYDATE   TIMESTAMP NOT NULL,

    LINENUMBER                   DECIMAL(32,16) NOT NULL,
    PRICE                        DECIMAL(32,6)  NOT NULL,
    QUANTITY                     DECIMAL(32,6)  NOT NULL,
    TRANSACTIONCURRENCYAMOUNT    DECIMAL(32,6)  NOT NULL,

    GENERALJOURNALENTRY BIGINT NOT NULL,
    BUDGETREGISTERLINERECID BIGINT NOT NULL,

    LEGALENTITYID  STRING NOT NULL,
    LEDGERDIMENSIONACCOUNTSTRUCTURE STRING NOT NULL,
    LEDGERDIMENSIONDISPLAYVALUE      STRING NOT NULL,

    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,

    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID              BIGINT     NOT NULL
)
USING delta
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2. (Optional) Add a comment about the omitted PRIMARY KEY
# ------------------------------------------------------------------
# The original T‑SQL defined a clustered primary key on 6 columns.
# Delta Lake does not enforce a primary key, but you can use a
# Delta constraint or a separate unique index if necessary:
#
# spark.sql(f"""
# ALTER TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBudgetRegisterEntryLineStaging`
# ADD CONSTRAINT PK_SMRBIBudgetRegisterEntryLineStaging
# UNIQUE (EXECUTIONID, LINENUMBER, BUDGETREGISTERLINERECID, LEGALENTITYID, DATAAREAID, PARTITION)
# """)

# -------------------------------------------
# 3. Verify the table schema
# -------------------------------------------
spark.sql(f"DESC TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBudgetRegisterEntryLineStaging`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
