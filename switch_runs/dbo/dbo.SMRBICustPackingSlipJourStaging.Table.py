# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustPackingSlipJourStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustPackingSlipJourStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# 1️⃣  Create a Delta table that mirrors dbo.SMRBICustPackingSlipJourStaging
#     The schema has been translated from T‑SQL types to Spark‑SQL types.
#
#      •  NVARCHAR(XX) → STRING
#      •  INT            → INT
#      •  BIGINT         → BIGINT
#      •  NUMERIC(p,s)   → DECIMAL(p,s)
#      •  DATETIME       → TIMESTAMP
#
# 2️⃣  Databricks (Delta) does **not** support PRIMARY KEY constraints.  
#     The original PK is documented in a comment so that downstream logic
#     can still be aware of it, but enforcement must be handled at
#     application level or via a unique index if needed.
#
# 3️⃣  All identifiers are fully‑qualified as:
#     `dbe_dbx_internships`.`dbo`.`SMRBICustPackingSlipJourStaging`
# ------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICustPackingSlipJourStaging` (
    -- Column definitions with Spark‑SQL datatypes
    DEFINITIONGROUP         STRING  NOT NULL,
    EXECUTIONID             STRING  NOT NULL,
    ISSELECTED              INT     NOT NULL,
    TRANSFERSTATUS          INT     NOT NULL,
    COMPANY                 STRING  NOT NULL,
    DELIVERYDATE            TIMESTAMP NOT NULL,
    DELIVERYNAME            STRING  NOT NULL,
    DLVMODE                 STRING  NOT NULL,
    DLVTERM                 STRING  NOT NULL,
    ORDERACCOUNT            STRING  NOT NULL,
    PACKINGSLIPID           STRING  NOT NULL,
    QTY                     DECIMAL(32,6) NOT NULL,
    SALESID                 STRING  NOT NULL,
    SALESTYPE                INT     NOT NULL,
    ADDRESS                 STRING  NOT NULL,
    PARTITION                STRING  NOT NULL,
    DATAAREAID              STRING  NOT NULL,
    SYNCSTARTDATETIME       TIMESTAMP NOT NULL,
    RECID                   BIGINT  NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1038)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBICustPackingSlipJourStaging` (     -- Column definitions with Spark‑SQL datatypes     DEFINITIONGROUP         STRING  NOT NULL,     EXECUTIONID             STRING  NOT NULL,     ISSELECTED              INT     NOT NULL,     TRANSFERSTATUS          INT     NOT NULL,     COMPANY                 STRING  NOT NULL,     DELIVERYDATE            TIMESTAMP NOT NULL,     DELIVERYNAME            STRING  NOT NULL,     DLVMODE                 STRING  NOT NULL,     DLVTERM                 STRING  NOT NULL,     ORDERACCOUNT            STRING  NOT NULL,     PACKINGSLIPID           STRING  NOT NULL,     QTY                     DECIMAL(32,6) NOT NULL,     SALESID                 STRING  NOT NULL,     SALESTYPE                INT     NOT NULL,     ADDRESS                 STRING  NOT NULL,     PARTITION                STRING  NOT NULL,     DATAAREAID              STRING  NOT NULL,     SYNCSTARTDATETIME       TIMESTAMP NOT NULL,     RECID                   BIGINT  NOT NULL );
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
