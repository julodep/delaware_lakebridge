# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventItemPendingPriceStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventItemPendingPriceStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------------------------
# Create the staging table `SMRBIInventItemPendingPriceStaging` in the
# target Unity Catalog catalog and schema.
#
# • All references are fully‑qualified: `dbe_dbx_internships`.`dbo`.`SMRBIInventItemPendingPriceStaging`.
# • Column definitions are mapped from T‑SQL to Spark SQL data types:
#     - NVARCHAR(n)  → STRING
#     - INT          → INT
#     - DATETIME     → TIMESTAMP
#     - NUMERIC(p,s) → DECIMAL(p,s)
# • The primary‑key and clustering hints that exist in the original T‑SQL
#   definition are not supported by Delta Lake.  
#   The constraint is omitted and a comment is added to remind the
#   operator that uniqueness must be enforced at the application level.
# ---------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventItemPendingPriceStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    FROMDATE TIMESTAMP NOT NULL,
    ITEMNUMBER STRING NOT NULL,
    PRICE DECIMAL(32,16) NOT NULL,
    PRICETYPE INT NOT NULL,
    PRICEQUANTITY DECIMAL(32,12) NOT NULL,
    PRICESITEID STRING NOT NULL,
    PRODUCTUNITSYMBOL STRING NOT NULL,
    PRODUCTCONFIGURATIONID STRING NOT NULL,
    PRODUCTCOLORID STRING NOT NULL,
    PRODUCTSIZEID STRING NOT NULL,
    PRODUCTSTYLEID STRING NOT NULL,
    COSTINGVERSIONID STRING NOT NULL,
    COMPANY STRING NOT NULL,
    INVENTDIMID STRING NOT NULL,
    UNITID STRING NOT NULL,
    PRICECALCID STRING NOT NULL,
    MARKUP DECIMAL(32,16) NOT NULL,
    PRICEUNIT DECIMAL(32,12) NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'comment' = 'Primary key constraint omitted: uniqueness is expected at the application layer.'
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
