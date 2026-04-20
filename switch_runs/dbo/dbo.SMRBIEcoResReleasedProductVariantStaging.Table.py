# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIEcoResReleasedProductVariantStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIEcoResReleasedProductVariantStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table in Delta Lake (Catalog = dbe_dbx_internships, Schema = dbo)
# ------------------------------------------------------------------
# Note:  The original T‑SQL definition contains a clustered PRIMARY KEY and
# various statistical options.
# Delta Lake (the storage layer used by Databricks) does not enforce primary
# keys or clustering in the same way as SQL Server.  We therefore omit those
# constraints from the DDL.  If uniqueness guarantees or indexing logic are
# required, they must be implemented in the application layer or via
# Delta Lake data‑quality checks.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIEcoResReleasedProductVariantStaging` (
    DEFINITIONGROUP      STRING NOT NULL,
    EXECUTIONID          STRING NOT NULL,
    ISSELECTED            INT   NOT NULL,
    TRANSFERSTATUS        INT   NOT NULL,
    ITEMNUMBER            STRING NOT NULL,
    PRODUCTVARIANTNUMBER STRING NOT NULL,
    INVENTDIMID           STRING NOT NULL,
    RETAILVARIANTID       STRING NOT NULL,
    DISTINCTPRODUCTVARIANT BIGINT NOT NULL,
    COMPANY               STRING NOT NULL,
    VARIANTCREATEDDATETIME TIMESTAMP NOT NULL,
    ECORESRELEASEDPRODUCTVARIANTRECID BIGINT NOT NULL,
    PARTITION             STRING NOT NULL,
    DATAAREAID            STRING NOT NULL,
    SYNCSTARTDATETIME    TIMESTAMP NOT NULL
)
USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
