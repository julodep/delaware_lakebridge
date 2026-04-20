# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendPackingSlipTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendPackingSlipTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the staging table SMRBIVendPackingSlipTransStaging
#  in the specified catalog and schema.
#
#  All object names are fully‑qualified:
#     <catalog>.<schema>.SMRBIVendPackingSlipTransStaging
#
#  T‑SQL data types → Spark SQL types mapping
#     nvarchar  → STRING
#     datetime  → TIMESTAMP
#     numeric   → DECIMAL(precision, scale)
#     int       → INT
#     bigint    → LONG
#
#  Primary key constraints are not supported in Delta Lake, so
#  the PK declaration is commented out.  You can recreate the
#  uniqueness requirement later with a unique index in a database
#  that does support it, or enforce it in application logic.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.SMRBIVendPackingSlipTransStaging (
    DEFINITIONGROUP                        STRING      NOT NULL,
    EXECUTIONID                           STRING      NOT NULL,
    ISSELECTED                            INT         NOT NULL,
    TRANSFERSTATUS                        INT         NOT NULL,
    DELIVERYDATE                          TIMESTAMP   NOT NULL,
    INVENTDIMID                           STRING      NOT NULL,
    ITEMID                                STRING      NOT NULL,
    LINENUM                               DECIMAL(32,16) NOT NULL,
    ORDERED                               DECIMAL(32,6)  NOT NULL,
    ORIGPURCHID                           STRING      NOT NULL,
    PACKINGSLIPID                         STRING      NOT NULL,
    PRICEUNIT                             DECIMAL(32,12) NOT NULL,
    PURCHASELINEEXPECTEDDELIVERYDATE      TIMESTAMP   NOT NULL,
    PURCHASELINELINENUMBER                BIGINT      NOT NULL,
    PURCHUNIT                             STRING      NOT NULL,
    QTY                                   DECIMAL(32,6)  NOT NULL,
    VENDPACKINGSLIPJOUR                   BIGINT      NOT NULL,
    COMPANY                               STRING      NOT NULL,
    PARTITION                             STRING      NOT NULL,
    DATAAREAID                            STRING      NOT NULL,
    SYNCSTARTDATETIME                     TIMESTAMP   NOT NULL,
    RECID                                 BIGINT      NOT NULL
    -- PRIMARY KEY (EXECUTIONID, DELIVERYDATE, INVENTDIMID, ITEMID,
    --              LINENUM, ORDERED, ORIGPURCHID, PACKINGSLIPID,
    --              PRICEUNIT, PURCHASELINEEXPECTEDDELIVERYDATE,
    --              PURCHASELINELINENUMBER, PURCHUNIT, QTY,
    --              VENDPACKINGSLIPJOUR, COMPANY, DATAAREAID, PARTITION)
)
""")

# COMMAND ----------

# Optional – confirm the table exists and show its schema
df = spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.SMRBIVendPackingSlipTransStaging")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1855)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `_placeholder_`.`_placeholder_`.SMRBIVendPackingSlipTransStaging (     DEFINITIONGROUP                        STRING      NOT NULL,     EXECUTIONID                           STRING      NOT NULL,     ISSELECTED                            INT         NOT NULL,     TRANSFERSTATUS                        INT         NOT NULL,     DELIVERYDATE                          TIMESTAMP   NOT NULL,     INVENTDIMID                           STRING      NOT NULL,     ITEMID                                STRING      NOT NULL,     LINENUM                               DECIMAL(32,16) NOT NULL,     ORDERED                               DECIMAL(32,6)  NOT NULL,     ORIGPURCHID                           STRING      NOT NULL,     PACKINGSLIPID                         STRING      NOT NULL,     PRICEUNIT                             DECIMAL(32,12) NOT NULL,     PURCHASELINEEXPECTEDDELIVERYDATE      TIMESTAMP   NOT NULL,     PURCHASELINELINENUMBER                BIGINT      NOT NULL,     PURCHUNIT                             STRING      NOT NULL,     QTY                                   DECIMAL(32,6)  NOT NULL,     VENDPACKINGSLIPJOUR                   BIGINT      NOT NULL,     COMPANY                               STRING      NOT NULL,     PARTITION                             STRING      NOT NULL,     DATAAREAID                            STRING      NOT NULL,     SYNCSTARTDATETIME                     TIMESTAMP   NOT NULL,     RECID                                 BIGINT      NOT NULL     -- PRIMARY KEY (EXECUTIONID, DELIVERYDATE, INVENTDIMID, ITEMID,     --              LINENUM, ORDERED, ORIGPURCHID, PACKINGSLIPID,     --              PRICEUNIT, PURCHASELINEEXPECTEDDELIVERYDATE,     --              PURCHASELINELINENUMBER, PURCHUNIT, QTY,     --              VENDPACKINGSLIPJOUR, COMPANY, DATAAREAID, PARTITION) )
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
