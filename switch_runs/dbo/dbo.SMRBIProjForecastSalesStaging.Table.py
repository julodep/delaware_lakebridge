# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjForecastSalesStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjForecastSalesStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the table (Delta Lake format, fully‑qualified name)
# ------------------------------------------------------------------
# The statement is wrapped in a multi‑line f‑string so that the
# placeholder `dbe_dbx_internships` and `dbo` are replaced at runtime.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastSalesStaging` (
    DEFINITIONGROUP        STRING   NOT NULL,   -- nvarchar(60)
    EXECUTIONID           STRING   NOT NULL,   -- nvarchar(90)
    ISSELECTED            INT      NOT NULL,   -- int
    TRANSFERSTATUS        INT      NOT NULL,   -- int
    ACTIVITYNUMBER        STRING   NOT NULL,   -- nvarchar(50)
    BUDGETCOMMENT         STRING   NOT NULL,   -- nvarchar(60)
    COSTPRICE             DECIMAL(32,6)   NOT NULL,   -- numeric(32,6)
    CURRENCY              STRING   NOT NULL,   -- nvarchar(3)
    CUSTOMERACCOUNTID     STRING   NOT NULL,   -- nvarchar(20)
    CUSTOMERGROUPID       STRING   NOT NULL,   -- nvarchar(10)
    ITEMGROUPID           STRING   NOT NULL,   -- nvarchar(10)
    ITEMID                STRING   NOT NULL,   -- nvarchar(20)
    MODELID               STRING   NOT NULL,   -- nvarchar(10)
    PRICEUNIT             DECIMAL(32,12) NOT NULL,   -- numeric(32,12)
    PROJECTCATEGORYID     STRING   NOT NULL,   -- nvarchar(30)
    PROJECTID             STRING   NOT NULL,   -- nvarchar(20)
    PROJECTLINEPROPERTYID STRING   NOT NULL,   -- nvarchar(10)
    PROJECTTRANSID        STRING   NOT NULL,   -- nvarchar(20)
    SALESPRICE            DECIMAL(32,6) NOT NULL,   -- numeric(32,6)
    SALESQUANTITY         DECIMAL(32,6) NOT NULL,   -- numeric(32,6)
    SALESUNITID           STRING   NOT NULL,   -- nvarchar(10)
    STARTDATE             TIMESTAMP NOT NULL,   -- datetime
    COMPANY               STRING   NOT NULL,   -- nvarchar(4)
    FORECASTEDREVENUE      INT      NOT NULL,   -- int
    PERIODKEYID           STRING   NOT NULL,   -- nvarchar(10)
    PROJECTTRANSACTIONID   STRING   NOT NULL,   -- nvarchar(20)
    EXPANDID              BIGINT   NOT NULL,   -- bigint
    PARTITION              STRING   NOT NULL,   -- nvarchar(20)
    DATAAREAID            STRING   NOT NULL,   -- nvarchar(4)
    SYNCSTARTDATETIME     TIMESTAMP NOT NULL,   -- datetime
    RECID                  BIGINT   NOT NULL    -- bigint
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Primary‑key definition (not enforced in Delta)
# ------------------------------------------------------------------
# The original T‑SQL defined a clustered primary key on
# ( EXECUTIONID, ACTIVITYNUMBER, DATAAREAID, PARTITION ).
# Delta Lake does not enforce primary keys, but you can still
# declare a logical constraint for documentation purposes if
# you are using a Databricks runtime 11.2+ that supports
# CHECK/UNIQUE constraints. The following is optional:
#
# spark.sql(f"""
# ALTER TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastSalesStaging`
# ADD CONSTRAINT pk_SMRBIProjForecastSalesStaging
# PRIMARY KEY (EXECUTIONID, ACTIVITYNUMBER, DATAAREAID, PARTITION)
# """)
#
# ------------------------------------------------------------------
# 3️⃣  Verify the schema (optional but helpful for debugging)
# ------------------------------------------------------------------
spark.sql(f"DESCRIBE FORMATTED `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastSalesStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2052)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS `_placeholder_`.`_placeholder_`.`SMRBIProjForecastSalesStaging` (     DEFINITIONGROUP        STRING   NOT NULL,   -- nvarchar(60)     EXECUTIONID           STRING   NOT NULL,   -- nvarchar(90)     ISSELECTED            INT      NOT NULL,   -- int     TRANSFERSTATUS        INT      NOT NULL,   -- int     ACTIVITYNUMBER        STRING   NOT NULL,   -- nvarchar(50)     BUDGETCOMMENT         STRING   NOT NULL,   -- nvarchar(60)     COSTPRICE             DECIMAL(32,6)   NOT NULL,   -- numeric(32,6)     CURRENCY              STRING   NOT NULL,   -- nvarchar(3)     CUSTOMERACCOUNTID     STRING   NOT NULL,   -- nvarchar(20)     CUSTOMERGROUPID       STRING   NOT NULL,   -- nvarchar(10)     ITEMGROUPID           STRING   NOT NULL,   -- nvarchar(10)     ITEMID                STRING   NOT NULL,   -- nvarchar(20)     MODELID               STRING   NOT NULL,   -- nvarchar(10)     PRICEUNIT             DECIMAL(32,12) NOT NULL,   -- numeric(32,12)     PROJECTCATEGORYID     STRING   NOT NULL,   -- nvarchar(30)     PROJECTID             STRING   NOT NULL,   -- nvarchar(20)     PROJECTLINEPROPERTYID STRING   NOT NULL,   -- nvarchar(10)     PROJECTTRANSID        STRING   NOT NULL,   -- nvarchar(20)     SALESPRICE            DECIMAL(32,6) NOT NULL,   -- numeric(32,6)     SALESQUANTITY         DECIMAL(32,6) NOT NULL,   -- numeric(32,6)     SALESUNITID           STRING   NOT NULL,   -- nvarchar(10)     STARTDATE             TIMESTAMP NOT NULL,   -- datetime     COMPANY               STRING   NOT NULL,   -- nvarchar(4)     FORECASTEDREVENUE      INT      NOT NULL,   -- int     PERIODKEYID           STRING   NOT NULL,   -- nvarchar(10)     PROJECTTRANSACTIONID   STRING   NOT NULL,   -- nvarchar(20)     EXPANDID              BIGINT   NOT NULL,   -- bigint     PARTITION              STRING   NOT NULL,   -- nvarchar(20)     DATAAREAID            STRING   NOT NULL,   -- nvarchar(4)     SYNCSTARTDATETIME     TIMESTAMP NOT NULL,   -- datetime     RECID                  BIGINT   NOT NULL    -- bigint ) USING DELTA
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
