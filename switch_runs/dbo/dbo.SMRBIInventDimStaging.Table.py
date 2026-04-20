# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventDimStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventDimStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# 1️⃣  Create the DL table `SMRBIInventDimStaging`
# ------------------------------------------------------------
# NOTE:  T-SQL types are mapped to Spark SQL equivalents:
#   – NVARCHAR   -> STRING
#   – DATETIME   -> TIMESTAMP
#   – INT        -> INT
# ------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventDimStaging` (
    DEFINITIONGROUP              STRING     NOT NULL,
    EXECUTIONID                  STRING     NOT NULL,
    ISSELECTED                   INT        NOT NULL,
    TRANSFERSTATUS               INT        NOT NULL,
    CONFIGID                     STRING     NOT NULL,
    INVENTBATCHID                STRING     NOT NULL,
    INVENTCOLORID                STRING     NOT NULL,
    INVENTDIMID                  STRING     NOT NULL,
    INVENTLOCATIONID             STRING     NOT NULL,
    INVENTSITEID                 STRING     NOT NULL,
    INVENTSIZEID                 STRING     NOT NULL,
    INVENTSTATUSID               STRING     NOT NULL,
    INVENTSTYLEID                STRING     NOT NULL,
    WMSLOCATIONID                STRING     NOT NULL,
    COMPANY                      STRING     NOT NULL,
    INVENTDIMMODIFIEDDATETIME    TIMESTAMP  NOT NULL,
    INVENTSERIALID               STRING     NOT NULL,
    PARTITION                    STRING     NOT NULL,
    DATAAREAID                   STRING     NOT NULL,
    SYNCSTARTDATETIME            TIMESTAMP  NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------
# 2️⃣  Verify that the table was created
# ------------------------------------------------------------
spark.sql(f"DESCRIBE DETAIL `dbe_dbx_internships`.`dbo`.`SMRBIInventDimStaging`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
