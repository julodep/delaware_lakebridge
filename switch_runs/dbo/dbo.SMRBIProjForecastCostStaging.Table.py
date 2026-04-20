# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjForecastCostStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjForecastCostStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create table `SMRBIProjForecastCostStaging` in the target catalog/schema
# ------------------------------------------------------------------
# NOTE:
#   * Delta Lake (Databricks) does not enforce PRIMARY KEY constraints.
#     The original T‑SQL definition required a clustered primary key over
#     (TRANSID, EXECUTIONID, DATAAREAID, PARTITION).  If you need to
#     guarantee uniqueness you need to add a business rule in downstream
#     ETL logic or use a Delta table *unique key* via a change‑log.
#   * All identifiers are fully‑qualified: `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastCostStaging`
#   * Data types have been mapped to Spark SQL equivalents.
#
# ----------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjForecastCostStaging` (
    SALESCURRENCY STRING NOT NULL,
    LINEPROPERTY STRING NOT NULL,
    PROJECTID STRING NOT NULL,
    TRANSID STRING NOT NULL,
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    ACTIVITYNUMBER STRING NOT NULL,
    CATEGORY STRING NOT NULL,
    COSTPRICE DECIMAL(32,6) NOT NULL,
    FORECASTMODEL STRING NOT NULL,
    QUANTITY DECIMAL(32,6) NOT NULL,
    SALESPRICE DECIMAL(32,6) NOT NULL,
    PROJECTDATE TIMESTAMP NOT NULL,
    DESCRIPTION STRING NOT NULL,
    COMPANY STRING NOT NULL,
    WORKER BIGINT NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)  -- Primary key omitted – Delta Lake doesn’t enforce PKs
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
