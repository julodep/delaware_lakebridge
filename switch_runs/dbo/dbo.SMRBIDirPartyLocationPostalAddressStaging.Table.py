# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDirPartyLocationPostalAddressStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDirPartyLocationPostalAddressStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table in Delta Lake
# ------------------------------------------------------------------
#   Oracle: CREATE TABLE [dbo].[SMRBIDirPartyLocationPostalAddressStaging] ...
#   Spark:  CREATE TABLE  `<catalog>`.`<schema>`.`SMRBIDirPartyLocationPostalAddressStaging` ...
#
# Data‑type mapping (SQL Server → Spark SQL)
#   NVARCHAR(n)  → STRING
#   INT          → INT
#   BIGINT       → BIGINT
#   DATETIME     → TIMESTAMP
#
# Note:  Primary key constraints (CLUSTERED PK) and the WITH ( … ) table
#   options are SQL Server only.  Delta Lake does not enforce uniqueness
#   automatically, so we leave them as comments for documentation
#   purposes only.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIDirPartyLocationPostalAddressStaging (
    DEFINITIONGROUP      STRING,
    EXECUTIONID          STRING,
    ISSELECTED           INT,
    TRANSFERSTATUS       INT,
    ISPRIMARY            INT,
    PARTYNUMBER          STRING,
    ADDRESS              STRING,
    CITY                 STRING,
    COUNTRYREGIONID      STRING,
    VALIDTO              TIMESTAMP,
    ZIPCODE              STRING,
    VALIDFROM            TIMESTAMP,
    PARTITION            STRING,
    SYNCSTARTDATETIME    TIMESTAMP,
    RECID                BIGINT
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Optional: create a unique index for reference
# ------------------------------------------------------------------
# In pure Delta Lake we cannot define a primary key, but if you want
# a unique index for query optimisation you can create a materialized
# view or a table partitioned by the PK columns.  The following is
# just a placeholder to illustrate that this table may have a
# composite primary key in the source system.
#
# Example (if you need to maintain uniqueness manually):
#   spark.sql(f"""
#   CREATE UNIQUE INDEX IF NOT EXISTS idx_pk
#   ON `dbe_dbx_internships`.`dbo`.SMRBIDirPartyLocationPostalAddressStaging
#   (EXECUTIONID, ISPRIMARY, PARTYNUMBER, ADDRESS, CITY, COUNTRYREGIONID,
#    VALIDTO, ZIPCODE, VALIDFROM, PARTITION);
#   """)
#
# But let us keep the definition simple and rely on application logic
# to enforce uniqueness if required.

print("Table `SMRBIDirPartyLocationPostalAddressStaging` created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
