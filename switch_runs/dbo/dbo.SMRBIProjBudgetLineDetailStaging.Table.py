# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjBudgetLineDetailStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjBudgetLineDetailStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the staging table in Delta Lake
# --------------------------------------------------------------
# NOTE:  In Delta Lake a PRIMARY KEY cannot be enforced – the T‑SQL
#   definition `PRIMARY KEY (EXECUTIONID, PROJBUDGETLINE, DATAAREAID, PARTITION)`
#   is preserved only as documentation.  If you want clustering or
#   partitioning you can add `CLUSTERED BY` or `PARTITIONED BY` clauses.
#
#   The `dbe_dbx_internships` and `dbo` placeholders are expected to be
#   defined in the notebook (e.g. as widgets or environment variables).
#   They will be interpolated by an f‑string below.
# --------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjBudgetLineDetailStaging`
(
    `DEFINITIONGROUP`           STRING      NOT NULL,   -- nvarchar(60)
    `EXECUTIONID`               STRING      NOT NULL,   -- nvarchar(90)
    `ISSELECTED`                INT         NOT NULL,   -- int
    `TRANSFERSTATUS`            INT         NOT NULL,   -- int
    `COSTPRICE`                 DECIMAL(32,6) NOT NULL,   -- numeric(32,6)
    `HCMWORKER`                 BIGINT      NOT NULL,   -- bigint
    `INVENTDIMID`               STRING      NOT NULL,   -- nvarchar(20)
    `ITEMID`                    STRING      NOT NULL,   -- nvarchar(20)
    `PROJBUDGETLINE`            BIGINT      NOT NULL,   -- bigint
    `PROJBUDGETREVISIONLINE`    BIGINT      NOT NULL,   -- bigint
    `QUANTITY`                  DECIMAL(32,6) NOT NULL,   -- numeric(32,6)
    `RESOURCE_`                 BIGINT      NOT NULL,   -- bigint
    `RESOURCECATEGORY`          BIGINT      NOT NULL,   -- bigint
    `SALESCATEGORY`             BIGINT      NOT NULL,   -- bigint
    `SALESPRICE`                DECIMAL(32,6) NOT NULL,   -- numeric(32,6)
    `SALESUNITID`               STRING      NOT NULL,   -- nvarchar(10)
    `PARTITION`                 STRING      NOT NULL,   -- nvarchar(20)
    `DATAAREAID`                STRING      NOT NULL,   -- nvarchar(4)
    `SYNCSTARTDATETIME`         TIMESTAMP   NOT NULL,   -- datetime
    `RECID`                     BIGINT      NOT NULL    -- bigint
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
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1498)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIProjBudgetLineDetailStaging` (     `DEFINITIONGROUP`           STRING      NOT NULL,   -- nvarchar(60)     `EXECUTIONID`               STRING      NOT NULL,   -- nvarchar(90)     `ISSELECTED`                INT         NOT NULL,   -- int     `TRANSFERSTATUS`            INT         NOT NULL,   -- int     `COSTPRICE`                 DECIMAL(32,6) NOT NULL,   -- numeric(32,6)     `HCMWORKER`                 BIGINT      NOT NULL,   -- bigint     `INVENTDIMID`               STRING      NOT NULL,   -- nvarchar(20)     `ITEMID`                    STRING      NOT NULL,   -- nvarchar(20)     `PROJBUDGETLINE`            BIGINT      NOT NULL,   -- bigint     `PROJBUDGETREVISIONLINE`    BIGINT      NOT NULL,   -- bigint     `QUANTITY`                  DECIMAL(32,6) NOT NULL,   -- numeric(32,6)     `RESOURCE_`                 BIGINT      NOT NULL,   -- bigint     `RESOURCECATEGORY`          BIGINT      NOT NULL,   -- bigint     `SALESCATEGORY`             BIGINT      NOT NULL,   -- bigint     `SALESPRICE`                DECIMAL(32,6) NOT NULL,   -- numeric(32,6)     `SALESUNITID`               STRING      NOT NULL,   -- nvarchar(10)     `PARTITION`                 STRING      NOT NULL,   -- nvarchar(20)     `DATAAREAID`                STRING      NOT NULL,   -- nvarchar(4)     `SYNCSTARTDATETIME`         TIMESTAMP   NOT NULL,   -- datetime     `RECID`                     BIGINT      NOT NULL    -- bigint ) USING DELTA
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
