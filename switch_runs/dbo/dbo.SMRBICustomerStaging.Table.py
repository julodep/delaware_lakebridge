# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustomerStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustomerStaging.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------------------
# 1️⃣  Convert the T‑SQL CREATE TABLE statement to a Databricks‑friendly
#     Spark SQL command.  
#     * Use Spark SQL data‑type mapping.
#     * Replace BIGINT representation (T‑SQL 'BIGINT' → Spark 'BIGINT').
#     * Include `USING DELTA` to create a Delta table.
# ----------------------------------------------------------------------
spark.sql("""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICustomerStaging` (
    -- 1️⃣  Column definitions (data types mapped from T‑SQL)

    DEFINITIONGROUP          STRING           NOT NULL,
    EXECUTIONID              STRING           NOT NULL,
    ISSELECTED                INT              NOT NULL,
    TRANSFERSTATUS            INT              NOT NULL,
    COMMISSIONSALESGROUPID   STRING           NOT NULL,
    CUSTOMERACCOUNT         STRING           NOT NULL,
    NAME                    STRING           NOT NULL,
    CUSTOMERGROUPID          STRING           NOT NULL,
    PARTYNUMBER              STRING           NOT NULL,
    SALESSEGMENTID            STRING           NOT NULL,
    SALESSUBSEGMENTID         STRING           NOT NULL,
    DELIVERYADDRESSLOCATIONID STRING           NOT NULL,
    CUSTCLASSIFICATIONID     STRING           NOT NULL,
    CREATEDDATETIME1          TIMESTAMP       NOT NULL,
    CUSTOMERRECID            BIGINT           NOT NULL,
    DLVTERM                  STRING           NOT NULL,
    COMPANY                   STRING           NOT NULL,
    CITY                     STRING           NOT NULL,
    COUNTRYREGIONID          STRING           NOT NULL,
    ZIPCODE                  STRING           NOT NULL,
    ADDRESS                  STRING           NOT NULL,
    ADDRESSVALIDTO           TIMESTAMP       NOT NULL,
    ADDRESSVALIDFROM         TIMESTAMP       NOT NULL,
    DELIVERYVALIDTO          TIMESTAMP       NOT NULL,
    DELIVERYVALIDFROM        TIMESTAMP       NOT NULL,
    ONHOLDSTATUS             INT              NOT NULL,
    CREDITLIMITISMANDATORY   INT              NOT NULL,
    CREDITLIMIT              DECIMAL(32,6)   NOT NULL,
    MAINCONTACTWORKER        BIGINT           NOT NULL,
    DEFAULTDIMENSION         BIGINT           NOT NULL,
    PARTITION                 STRING           NOT NULL,
    YSLECOMPANYCHAINID       STRING           NOT NULL,
    YSLEGROUPDIMENSION       STRING           NOT NULL,
    DATAAREAID                STRING           NOT NULL,
    SYNCSTARTDATETIME        TIMESTAMP       NOT NULL

    -- 2️⃣  Primary‑key / clustering information from the original
    --     T‑SQL (`PRIMARY KEY CLUSTERED (…)`) cannot be expressed
    --     directly in Delta Lake schema.  If you need a unique
    --     constraint you should add a separate `ALTER TABLE
    --     ADD CONSTRAINT ...` statement after the table creation,
    --     or enforce uniqueness in your application logic.
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2487)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `catalog`.`schema`.`SMRBICustomerStaging` (     -- 1️⃣  Column definitions (data types mapped from T‑SQL)      DEFINITIONGROUP          STRING           NOT NULL,     EXECUTIONID              STRING           NOT NULL,     ISSELECTED                INT              NOT NULL,     TRANSFERSTATUS            INT              NOT NULL,     COMMISSIONSALESGROUPID   STRING           NOT NULL,     CUSTOMERACCOUNT         STRING           NOT NULL,     NAME                    STRING           NOT NULL,     CUSTOMERGROUPID          STRING           NOT NULL,     PARTYNUMBER              STRING           NOT NULL,     SALESSEGMENTID            STRING           NOT NULL,     SALESSUBSEGMENTID         STRING           NOT NULL,     DELIVERYADDRESSLOCATIONID STRING           NOT NULL,     CUSTCLASSIFICATIONID     STRING           NOT NULL,     CREATEDDATETIME1          TIMESTAMP       NOT NULL,     CUSTOMERRECID            BIGINT           NOT NULL,     DLVTERM                  STRING           NOT NULL,     COMPANY                   STRING           NOT NULL,     CITY                     STRING           NOT NULL,     COUNTRYREGIONID          STRING           NOT NULL,     ZIPCODE                  STRING           NOT NULL,     ADDRESS                  STRING           NOT NULL,     ADDRESSVALIDTO           TIMESTAMP       NOT NULL,     ADDRESSVALIDFROM         TIMESTAMP       NOT NULL,     DELIVERYVALIDTO          TIMESTAMP       NOT NULL,     DELIVERYVALIDFROM        TIMESTAMP       NOT NULL,     ONHOLDSTATUS             INT              NOT NULL,     CREDITLIMITISMANDATORY   INT              NOT NULL,     CREDITLIMIT              DECIMAL(32,6)   NOT NULL,     MAINCONTACTWORKER        BIGINT           NOT NULL,     DEFAULTDIMENSION         BIGINT           NOT NULL,     PARTITION                 STRING           NOT NULL,     YSLECOMPANYCHAINID       STRING           NOT NULL,     YSLEGROUPDIMENSION       STRING           NOT NULL,     DATAAREAID                STRING           NOT NULL,     SYNCSTARTDATETIME        TIMESTAMP       NOT NULL      -- 2️⃣  Primary‑key / clustering information from the original     --     T‑SQL (`PRIMARY KEY CLUSTERED (…)`) cannot be expressed     --     directly in Delta Lake schema.  If you need a unique     --     constraint you should add a separate `ALTER TABLE     --     ADD CONSTRAINT ...` statement after the table creation,     --     or enforce uniqueness in your application logic. ) USING DELTA
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
