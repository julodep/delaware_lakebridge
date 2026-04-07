# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.PriceAgreement.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.PriceAgreement.Table.sql`

# COMMAND ----------

# ----------------------------------------------------------------------------------
# Create the PriceAgreement table in the target Unity Catalog
# ----------------------------------------------------------------------------------
# 1. Data type mapping:
#    NVARCHAR      -> STRING
#    NUMERIC(p,s)  -> DECIMAL(p,s)
#    DATETIME      -> TIMESTAMP
#    NOT NULL      -> Spark SQL treats columns as nullable by default, so we
#                     specify NOT NULL in the CREATE TABLE statement.
#
# 2. Primary key:
#    The original T‑SQL does not explicitly declare a primary key, only
#    that the table is created on the PRIMARY filegroup. Unity Catalog does not
#    expose filegroups. If a unique constraint is required, add it afterwards
#    using ALTER TABLE ADD CONSTRAINT. For simplicity we keep the schema as is.
#
# ----------------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`PriceAgreement` (
    VendorCode   STRING  NOT NULL,
    ProductCode  STRING  NOT NULL,
    CompanyCode  STRING  NOT NULL,
    Amount       DECIMAL(32,6) NOT NULL,
    Currency     STRING  NOT NULL,
    FromDate     TIMESTAMP NOT NULL,
    ToDate       TIMESTAMP NOT NULL,
    QtyFrom      DECIMAL(32,6) NOT NULL,
    QtyTo        DECIMAL(32,6) NOT NULL,
    UnitId       STRING  NOT NULL,
    PriceUnit    DECIMAL(32,12) NOT NULL
)
""")

# COMMAND ----------

# If you need to enforce a unique key (for example, the combination of
# VendorCode, ProductCode, and CompanyCode), you can add:
# spark.sql(f"""
# ALTER TABLE `dbe_dbx_internships`.`datastore`.`PriceAgreement`
# ADD CONSTRAINT pk_price_agreement UNIQUE (VendorCode, ProductCode, CompanyCode)
# """)

# Verify creation
display(spark.sql(f"DESC TABLE `dbe_dbx_internships`.`datastore`.`PriceAgreement`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
