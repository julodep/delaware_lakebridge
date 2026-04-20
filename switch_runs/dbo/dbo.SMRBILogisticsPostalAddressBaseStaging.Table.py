# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBILogisticsPostalAddressBaseStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBILogisticsPostalAddressBaseStaging.Table.sql`

# COMMAND ----------

# =============================================================================
#  Databricks notebook to create the staging table
#  Target catalog:  `dbe_dbx_internships`
#  Target schema :  `dbo`
#  Fully‑qualified name: `dbe_dbx_internships`.`dbo`.`SMRBILogisticsPostalAddressBaseStaging`
#
#  T‑SQL CREATE TABLE statement:
#      - `[dbo].[SMRBILogisticsPostalAddressBaseStaging]`
#      - Columns:
#          DEFINITIONGROUP   NVARCHAR(60)    -> STRING
#          EXECUTIONID       NVARCHAR(90)    -> STRING
#          ISSELECTED        INT            -> INT
#          TRANSFERSTATUS    INT            -> INT
#          ADDRESS           NVARCHAR(250)   -> STRING
#          CITY              NVARCHAR(60)    -> STRING
#          COUNTRYREGIONID  NVARCHAR(10)    -> STRING
#          VALIDFROM         DATETIME        -> TIMESTAMP
#          VALIDTO           DATETIME        -> TIMESTAMP
#          ZIPCODE           NVARCHAR(10)    -> STRING
#          LOGISCTOCSPOSTALADDRESSBASERECID BIGINT -> BIGINT
#          PARTITION         NVARCHAR(20)    -> STRING
#          SYNCSTARTDATETIME DATETIME       -> TIMESTAMP
#          RECID             BIGINT         -> BIGINT
#
#  Notes
#  ----
#  * The primary key definition and index hints from T‑SQL (`PRIMARY KEY CLUSTERED` and
#    `WITH (…)`) are omitted because Delta Lake / Spark SQL manages uniqueness
#    differently.  If you need a uniqueness constraint you can enforce it with
#    a unique index in Spark (e.g., via `createUniqueIndex`) or by adding a
#    `UNIQUE` constraint in a later ALTER TABLE command.
#  * The column name `PARTITION` is a reserved word in Spark; we keep it as is
#    but wrap it in backticks when referenced.
#  * We expose the table as a Delta table (`CREATE OR REPLACE TABLE`) so that
#    it can participate in ACID transactions and time travel.
# =============================================================================

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBILogisticsPostalAddressBaseStaging`
(
    DEFINITIONGROUP STRING,
    EXECUTIONID     STRING,
    ISSELECTED      INT,
    TRANSFERSTATUS  INT,
    ADDRESS         STRING,
    CITY            STRING,
    COUNTRYREGIONID STRING,
    VALIDFROM       TIMESTAMP,
    VALIDTO         TIMESTAMP,
    ZIPCODE         STRING,
    LOGISCTOCSPOSTALADDRESSBASERECID BIGINT,
    `PARTITION`     STRING,
    SYNCSTARTDATETIME TIMESTAMP,
    RECID           BIGINT
)
USING DELTA
"""

)  # end of CREATE TABLE

# COMMAND ----------

# -------------------------------------------------------------------------------
#  Optional: create a view for easier querying
# -------------------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`dbo`.vw_SMRBILogisticsPostalAddressBaseStaging
AS
SELECT *
FROM `dbe_dbx_internships`.`dbo`.`SMRBILogisticsPostalAddressBaseStaging`
""")

# COMMAND ----------

# -------------------------------------------------------------------------------
#  Verify that the table was created
# -------------------------------------------------------------------------------
spark.sql(f"DESCRIBE dbe_dbx_internships.dbo.SMRBILogisticsPostalAddressBaseStaging").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
