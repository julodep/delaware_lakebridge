# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjInvoiceJourStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjInvoiceJourStaging.Table.sql`

# COMMAND ----------

# Create the SMRBIProjInvoiceJourStaging table in Databricks Delta Lake
# ------------------------------------------------------------
# Replace the placeholder values below with your actual catalog and schema names.
# ------------------------------------------------------------
catalog = "your_catalog_name"
schema  = "your_schema_name"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjInvoiceJourStaging` (
    `DEFINITIONGROUP`          STRING  NOT NULL,
    `EXECUTIONID`              STRING  NOT NULL,
    `ISSELECTED`               INT     NOT NULL,
    `TRANSFERSTATUS`           INT     NOT NULL,
    `CASHDISC`                 DECIMAL(32,6) NOT NULL,
    `CASHDISCCODE`             STRING  NOT NULL,
    `CASHDISCPERCENT`          DECIMAL(32,6) NOT NULL,
    `CASHDISCDATE`             TIMESTAMP NOT NULL,
    `CONTACTPERSONID_NO`       STRING  NOT NULL,
    `COSTVALUE`                DECIMAL(32,6) NOT NULL,
    `CURRENCYID`               STRING  NOT NULL,
    `CUSTPURCHASEORDER_NO`     STRING  NOT NULL,
    `CUSTREF_NO`               STRING  NOT NULL,
    `DEFAULTDIMENSION`         BIGINT  NOT NULL,
    `DELIVERYNAME`             STRING  NOT NULL,
    `DELIVERYPOSTALADDRESS`    BIGINT  NOT NULL,
    `DESCRIPTION`              STRING  NOT NULL,
    `DIRECTDEBITMANDATE`       BIGINT  NOT NULL,
    `DLVMODE`                  STRING  NOT NULL,
    `DLVTERM`                  STRING  NOT NULL,
    `DOCUMENTDATE_W`           TIMESTAMP NOT NULL,
    `DUEDATE`                  TIMESTAMP NOT NULL,
    `EINVOICEACCOUNTCODE`      STRING  NOT NULL,
    `EINVOICELINESPECIFIC`     INT     NOT NULL,
    `ENDDISC`                  DECIMAL(32,6) NOT NULL,
    `ENTERPRISENUMBER`         STRING  NOT NULL,
    `EUSALESLIST`              STRING  NOT NULL,
    `EXCHRATE`                 DECIMAL(32,16) NOT NULL,
    `EXCHRATESECONDARY`        DECIMAL(32,16) NOT NULL,
    `GIROTYPE`                 INT     NOT NULL,
    `INTERCOMPANYPOSTED`       INT     NOT NULL,
    `INTRASTATADDVALUE_LV`     DECIMAL(32,6) NOT NULL,
    `INTRASTATDISPATCHID`      STRING  NOT NULL,
    `INVOICEACCOUNT`           STRING  NOT NULL,
    `INVOICEAMOUNT`            DECIMAL(32,6) NOT NULL,
    `INVOICEDATE`              TIMESTAMP NOT NULL,
    `INVOICENUMBERINGCODE_LT`  STRING  NOT NULL,
    `INVOICEROUNDOFF`          DECIMAL(32,6) NOT NULL,
    `INVOICETYPE_MY`           INT     NOT NULL,
    `ISPROFORMA`               INT     NOT NULL,
    `LANGUAGEID`               STRING  NOT NULL,
    `LEDGERVOUCHER`            STRING  NOT NULL,
    `LISTCODEID`               INT     NOT NULL,
    `NARRATION_BR`             STRING  NOT NULL,
    `NOTETYPE_MY`              INT     NOT NULL,
    `NUMBERSEQUENCEGROUPID`    STRING  NOT NULL,
    `ONACCOUNTAMOUNT`          DECIMAL(32,6) NOT NULL,
    `ORDERACCOUNT`             STRING  NOT NULL,
    `PARMID`                   STRING  NOT NULL,
    `PARTITION1`               BIGINT  NOT NULL,
    `PAYMDAYID`                STRING  NOT NULL,
    `PAYMENT`                  STRING  NOT NULL,
    `PAYMENTSCHED`             STRING  NOT NULL,
    `PORT`                     STRING  NOT NULL,
    `PAYMID`                   STRING  NOT NULL,
    `POSTINGJOURNALID`         STRING  NOT NULL,
    `POSTINGPROFILE`           STRING  NOT NULL,
    `PRINTEDORIGINALS`         INT     NOT NULL,
    `PROJGROUPID`              STRING  NOT NULL,
    `PROJINVOICEID`            STRING  NOT NULL,
    `PROJINVOICEPROJID`        STRING  NOT NULL,
    `PROJINVOICETYPE`          INT     NOT NULL,
    `PSAENDDATEMAXINVOICEID`   STRING  NOT NULL,
    `PROPOSALID`               STRING  NOT NULL,
    `PSAINVOICEFORMATS`        INT     NOT NULL,
    `QTY`                      DECIMAL(32,6) NOT NULL,
    `REASONTABLEREF`           BIGINT  NOT NULL,
    `RECID1`                   BIGINT  NOT NULL,
    `SALESDATE_CZ`             TIMESTAMP NOT NULL,
    `RECVERSION1`              INT     NOT NULL,
    `SALESORDERBALANCE`        DECIMAL(32,6) NOT NULL,
    `SENTELECTRONICALLY`       INT     NOT NULL,
    `SMASPECINDEXCALC`         INT     NOT NULL,
    `SOURCEDOCUMENTHEADER`    BIGINT  NOT NULL,
    `SUMMARKUP`                DECIMAL(32,6) NOT NULL,
    `SUMLINEDISC`              DECIMAL(32,6) NOT NULL,
    `SUMTAX`                   DECIMAL(32,6) NOT NULL,
    `TAXGROUPID`               STRING  NOT NULL,
    `TAXINFORMATION_IN`       BIGINT  NOT NULL,
    `TAXPRINTONINVOICE`        INT     NOT NULL,
    `TAXSPECIFYBYLINE`         INT     NOT NULL,
    `TAXSPECIFYTOTAL`          INT     NOT NULL,
    `TRANSPORTATIONDOCUMENT`   BIGINT  NOT NULL,
    `TRIANGULATION`            INT     NOT NULL,
    `VOLUME`                   DECIMAL(32,6) NOT NULL,
    `VATNUM`                   STRING  NOT NULL,
    `VOUCHERNUMBERSEQUENCETABLE` BIGINT  NOT NULL,
    `WEIGHT`                   DECIMAL(32,12) NOT NULL,
    `WHOISAUTHOR_LT`           INT     NOT NULL,
    `PARTITION`                STRING  NOT NULL,
    `DATAAREAID`               STRING  NOT NULL,
    `SYNCSTARTDATETIME`        TIMESTAMP NOT NULL,
    `RECID`                    BIGINT  NOT NULL
    -- PRIMARY KEY is omitted because Delta Lake does not support a
    -- traditional primary‑key constraint; enforce uniqueness
    -- via a unique index or subsequent data‑quality checks if required.
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 4980)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `your_catalog_name`.`your_schema_name`.`SMRBIProjInvoiceJourStaging` (     `DEFINITIONGROUP`          STRING  NOT NULL,     `EXECUTIONID`              STRING  NOT NULL,     `ISSELECTED`               INT     NOT NULL,     `TRANSFERSTATUS`           INT     NOT NULL,     `CASHDISC`                 DECIMAL(32,6) NOT NULL,     `CASHDISCCODE`             STRING  NOT NULL,     `CASHDISCPERCENT`          DECIMAL(32,6) NOT NULL,     `CASHDISCDATE`             TIMESTAMP NOT NULL,     `CONTACTPERSONID_NO`       STRING  NOT NULL,     `COSTVALUE`                DECIMAL(32,6) NOT NULL,     `CURRENCYID`               STRING  NOT NULL,     `CUSTPURCHASEORDER_NO`     STRING  NOT NULL,     `CUSTREF_NO`               STRING  NOT NULL,     `DEFAULTDIMENSION`         BIGINT  NOT NULL,     `DELIVERYNAME`             STRING  NOT NULL,     `DELIVERYPOSTALADDRESS`    BIGINT  NOT NULL,     `DESCRIPTION`              STRING  NOT NULL,     `DIRECTDEBITMANDATE`       BIGINT  NOT NULL,     `DLVMODE`                  STRING  NOT NULL,     `DLVTERM`                  STRING  NOT NULL,     `DOCUMENTDATE_W`           TIMESTAMP NOT NULL,     `DUEDATE`                  TIMESTAMP NOT NULL,     `EINVOICEACCOUNTCODE`      STRING  NOT NULL,     `EINVOICELINESPECIFIC`     INT     NOT NULL,     `ENDDISC`                  DECIMAL(32,6) NOT NULL,     `ENTERPRISENUMBER`         STRING  NOT NULL,     `EUSALESLIST`              STRING  NOT NULL,     `EXCHRATE`                 DECIMAL(32,16) NOT NULL,     `EXCHRATESECONDARY`        DECIMAL(32,16) NOT NULL,     `GIROTYPE`                 INT     NOT NULL,     `INTERCOMPANYPOSTED`       INT     NOT NULL,     `INTRASTATADDVALUE_LV`     DECIMAL(32,6) NOT NULL,     `INTRASTATDISPATCHID`      STRING  NOT NULL,     `INVOICEACCOUNT`           STRING  NOT NULL,     `INVOICEAMOUNT`            DECIMAL(32,6) NOT NULL,     `INVOICEDATE`              TIMESTAMP NOT NULL,     `INVOICENUMBERINGCODE_LT`  STRING  NOT NULL,     `INVOICEROUNDOFF`          DECIMAL(32,6) NOT NULL,     `INVOICETYPE_MY`           INT     NOT NULL,     `ISPROFORMA`               INT     NOT NULL,     `LANGUAGEID`               STRING  NOT NULL,     `LEDGERVOUCHER`            STRING  NOT NULL,     `LISTCODEID`               INT     NOT NULL,     `NARRATION_BR`             STRING  NOT NULL,     `NOTETYPE_MY`              INT     NOT NULL,     `NUMBERSEQUENCEGROUPID`    STRING  NOT NULL,     `ONACCOUNTAMOUNT`          DECIMAL(32,6) NOT NULL,     `ORDERACCOUNT`             STRING  NOT NULL,     `PARMID`                   STRING  NOT NULL,     `PARTITION1`               BIGINT  NOT NULL,     `PAYMDAYID`                STRING  NOT NULL,     `PAYMENT`                  STRING  NOT NULL,     `PAYMENTSCHED`             STRING  NOT NULL,     `PORT`                     STRING  NOT NULL,     `PAYMID`                   STRING  NOT NULL,     `POSTINGJOURNALID`         STRING  NOT NULL,     `POSTINGPROFILE`           STRING  NOT NULL,     `PRINTEDORIGINALS`         INT     NOT NULL,     `PROJGROUPID`              STRING  NOT NULL,     `PROJINVOICEID`            STRING  NOT NULL,     `PROJINVOICEPROJID`        STRING  NOT NULL,     `PROJINVOICETYPE`          INT     NOT NULL,     `PSAENDDATEMAXINVOICEID`   STRING  NOT NULL,     `PROPOSALID`               STRING  NOT NULL,     `PSAINVOICEFORMATS`        INT     NOT NULL,     `QTY`                      DECIMAL(32,6) NOT NULL,     `REASONTABLEREF`           BIGINT  NOT NULL,     `RECID1`                   BIGINT  NOT NULL,     `SALESDATE_CZ`             TIMESTAMP NOT NULL,     `RECVERSION1`              INT     NOT NULL,     `SALESORDERBALANCE`        DECIMAL(32,6) NOT NULL,     `SENTELECTRONICALLY`       INT     NOT NULL,     `SMASPECINDEXCALC`         INT     NOT NULL,     `SOURCEDOCUMENTHEADER`    BIGINT  NOT NULL,     `SUMMARKUP`                DECIMAL(32,6) NOT NULL,     `SUMLINEDISC`              DECIMAL(32,6) NOT NULL,     `SUMTAX`                   DECIMAL(32,6) NOT NULL,     `TAXGROUPID`               STRING  NOT NULL,     `TAXINFORMATION_IN`       BIGINT  NOT NULL,     `TAXPRINTONINVOICE`        INT     NOT NULL,     `TAXSPECIFYBYLINE`         INT     NOT NULL,     `TAXSPECIFYTOTAL`          INT     NOT NULL,     `TRANSPORTATIONDOCUMENT`   BIGINT  NOT NULL,     `TRIANGULATION`            INT     NOT NULL,     `VOLUME`                   DECIMAL(32,6) NOT NULL,     `VATNUM`                   STRING  NOT NULL,     `VOUCHERNUMBERSEQUENCETABLE` BIGINT  NOT NULL,     `WEIGHT`                   DECIMAL(32,12) NOT NULL,     `WHOISAUTHOR_LT`           INT     NOT NULL,     `PARTITION`                STRING  NOT NULL,     `DATAAREAID`               STRING  NOT NULL,     `SYNCSTARTDATETIME`        TIMESTAMP NOT NULL,     `RECID`                    BIGINT  NOT NULL     -- PRIMARY KEY is omitted because Delta Lake does not support a     -- traditional primary‑key constraint; enforce uniqueness     -- via a unique index or subsequent data‑quality checks if required. )
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
