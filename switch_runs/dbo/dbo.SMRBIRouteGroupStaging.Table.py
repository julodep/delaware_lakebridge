# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIRouteGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIRouteGroupStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
#  Create the staging table `SMRBIRouteGroupStaging`
# -------------------------------------------------------------------------
# The original T‑SQL statement defines a table with the following columns:
#
#   DEFINITIONGROUP     NVARCHAR(60)      NOT NULL
#   EXECUTIONID         NVARCHAR(90)      NOT NULL
#   ISSELECTED          INT               NOT NULL
#   TRANSFERSTATUS      INT               NOT NULL
#   GROUPID             NVARCHAR(10)      NOT NULL
#   GROUPNAME           NVARCHAR(60)      NOT NULL
#   COMPANY             NVARCHAR(4)       NOT NULL
#   PARTITION           NVARCHAR(20)      NOT NULL
#   DATAAREAID          NVARCHAR(4)       NOT NULL
#   SYNCSTARTDATETIME  DATETIME          NOT NULL
#
# In Databricks (Spark SQL) we map the data types as follows:
#   NVARCHAR(...)   -> STRING
#   INT             -> INT
#   DATETIME        -> TIMESTAMP
#
# Spark does not support explicit PRIMARY KEY or clustering keys in a
# standard DELTA table definition, so we create the table without these
# constraints and add a comment for future reference.
#
# The fully‑qualified table name is:
#   dbe_dbx_internships.dbo.SMRBIRouteGroupStaging
#
# -------------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIRouteGroupStaging` (
    DEFINITIONGROUP STRING,
    EXECUTIONID     STRING,
    ISSELECTED      INT,
    TRANSFERSTATUS  INT,
    GROUPID         STRING,
    GROUPNAME       STRING,
    COMPANY         STRING,
    PARTITION       STRING,
    DATAAREAID      STRING,
    SYNCSTARTDATETIME TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# -----------------------------------------------
# Optional: show the newly created table schema
# -----------------------------------------------
display(spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIRouteGroupStaging`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
